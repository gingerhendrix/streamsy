import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Effect, Exit } from "effect";
import { importLegacyIssueStore, migrateViewStore, VIEW_SCHEMA_VERSION } from "./sqlite-schema.ts";
import { sqliteService } from "./sqlite.ts";

test("view migrations are independent and idempotent", () => {
  const database = new Database(":memory:");
  migrateViewStore(database, 1);
  migrateViewStore(database, 2);
  expect(
    database
      .query<{ version: number }, []>(
        "SELECT MAX(version) version FROM streamsy_view_schema_version",
      )
      .get()?.version,
  ).toBe(VIEW_SCHEMA_VERSION);
  expect(
    database
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='table'")
      .all()
      .map((row) => row.name),
  ).toContain("streamsy_view_checkpoint_manifests");
  database.close();
});

test("imports accepted Slice 1 rows once without removing legacy tables", async () => {
  const database = new Database(":memory:");
  database.run(
    "CREATE TABLE view_rows(workspace_id TEXT,row_key TEXT,value TEXT,PRIMARY KEY(workspace_id,row_key));CREATE TABLE reducer_state(workspace_id TEXT,row_key TEXT,value TEXT,PRIMARY KEY(workspace_id,row_key));CREATE TABLE view_progress(workspace_id TEXT PRIMARY KEY,checkpoint TEXT,published TEXT,next_sequence INTEGER);INSERT INTO view_rows VALUES('main','i1','{\"issueId\":\"i1\"}');INSERT INTO reducer_state SELECT * FROM view_rows;INSERT INTO view_progress VALUES('main','offset-1',NULL,1)",
  );
  migrateViewStore(database, 1);
  const config = {
    planName: "issues",
    planHash: "hash",
    partition: "main",
    sourceId: "events",
    relationId: "rows",
    reducerId: "lifecycle",
    reducerVersion: 1,
  };
  importLegacyIssueStore(database, config, 2);
  importLegacyIssueStore(database, config, 3);
  const store = sqliteService(database);
  expect(await Effect.runPromise(store.getRow({ ...config, id: "rows" }, "i1"))).toEqual({
    issueId: "i1",
  });
  expect(await Effect.runPromise(store.sourceProgress(config))).toBe("offset-1");
  expect(database.query("SELECT 1 FROM view_rows").get()).not.toBeNull();
  database.close();
});

test("reports malformed durable JSON as typed restore poison", async () => {
  const database = new Database(":memory:");
  migrateViewStore(database, 1);
  database.run("INSERT INTO streamsy_view_values VALUES('rows','p','x','r','\"k\"','{bad')");
  const exit = await Effect.runPromiseExit(
    sqliteService(database).getRow(
      { planName: "p", planHash: "h", partition: "x", sourceId: "s", id: "r" },
      "k",
    ),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  database.close();
});
