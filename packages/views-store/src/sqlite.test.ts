import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { viewStoreConformance } from "./conformance.ts";
import { migrateViewStore } from "./sqlite-schema.ts";
import { sqliteService } from "./sqlite.ts";

const factory = async () => {
  const filename = join(mkdtempSync(join(tmpdir(), "views-store-")), "view.sqlite");
  const open = async () => {
    const database = new Database(filename, { create: true });
    database.run("PRAGMA foreign_keys=ON");
    migrateViewStore(database, 1);
    return {
      store: sqliteService(database),
      restart: async () => {
        database.close(false);
        return open();
      },
      close: async () => database.close(false),
    };
  };
  return open();
};
viewStoreConformance("SQLite", factory);
