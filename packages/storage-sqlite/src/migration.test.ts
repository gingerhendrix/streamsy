import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { migrate, SCHEMA_VERSION } from "./index.ts";

describe("migrations", () => {
  test("apply the current schema version and tables", () => {
    const db = new Database(":memory:");
    migrate(db);
    const version = db
      .query<{ version: number }, []>("select max(version) as version from streamsy_schema_version")
      .get();
    expect(version?.version).toBe(SCHEMA_VERSION);

    const tables = db
      .query<{ name: string }, []>("select name from sqlite_master where type = 'table'")
      .all()
      .map((row) => row.name);
    expect(tables).toContain("streamsy_streams");
    expect(tables).toContain("streamsy_messages");
    expect(tables).toContain("streamsy_producers");

    const streamColumns = db
      .query<{ name: string }, []>("pragma table_info(streamsy_streams)")
      .all()
      .map((row) => row.name);
    expect(streamColumns).not.toContain(["child", "ref", "count"].join("_"));
    db.close();
  });

  test("are idempotent when run repeatedly", () => {
    const db = new Database(":memory:");
    migrate(db);
    migrate(db);
    const count = db
      .query<{ n: number }, []>("select count(*) as n from streamsy_schema_version")
      .get();
    expect(count?.n).toBe(SCHEMA_VERSION);
    db.close();
  });
});
