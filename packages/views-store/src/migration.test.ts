import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Context, Effect, Exit, Layer, ManagedRuntime } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import { importLegacyIssueStore, migrateViewStore, VIEW_SCHEMA_VERSION } from "./sqlite-schema.ts";
import { sqliteService } from "./sqlite.ts";

const sqliteClientLayer = (filename: string) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* SqliteClient.make({ filename, create: true });
      yield* client.unsafe<Record<string, never>>("PRAGMA foreign_keys = ON").pipe(Effect.asVoid);
      return Context.empty().pipe(
        Context.add(SqliteClient.SqliteClient, client),
        Context.add(SqlClient.SqlClient, client),
      );
    }),
  ).pipe(Layer.provide(Reactivity.layer));

let nextFilenameId = 0;

const runStore = <A, E>(
  filename: string,
  effect: (store: ReturnType<typeof sqliteService>) => Effect.Effect<A, E>,
) => {
  const runtime = ManagedRuntime.make(sqliteClientLayer(filename));
  const store = runtime.runSync(Effect.map(SqlClient.SqlClient, sqliteService));
  return runtime.runPromise(effect(store)).finally(() => runtime.dispose());
};

test("view migrations are independent and idempotent", () => {
  const database = new Database(":memory:");
  migrateViewStore(database);
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

test("imports accepted Slice 1 rows once without removing legacy tables", () => {
  const filename = `/tmp/views-store-import-${process.pid}-${nextFilenameId}.sqlite`;
  nextFilenameId += 1;
  const database = new Database(filename, { create: true });
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
  expect(database.query("SELECT 1 FROM view_rows").get()).not.toBeNull();
  database.close(false);
  return runStore(filename, (store) =>
    Effect.gen(function* () {
      expect(yield* store.getRow({ ...config, id: "rows" }, "i1")).toEqual({ issueId: "i1" });
      expect(yield* store.sourceProgress(config)).toBe("offset-1");
    }),
  );
});

test("reports malformed durable JSON as typed restore poison", () => {
  const filename = `/tmp/views-store-poison-${process.pid}-${nextFilenameId}.sqlite`;
  nextFilenameId += 1;
  const database = new Database(filename, { create: true });
  migrateViewStore(database, 1);
  database.run("INSERT INTO streamsy_view_values VALUES('rows','p','x','r','\"k\"','{bad')");
  database.close(false);
  return runStore(filename, (store) =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        store.getRow(
          { planName: "p", planHash: "h", partition: "x", sourceId: "s", id: "r" },
          "k",
        ),
      );
      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );
});
