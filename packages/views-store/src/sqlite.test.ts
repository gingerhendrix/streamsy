import { Database } from "bun:sqlite";
import { viewStoreConformance } from "./conformance.ts";
import { migrateViewStore } from "./sqlite-schema.ts";
import { sqliteService } from "./sqlite.ts";

let nextDatabaseId = 0;
const factory = () => {
  const filename = `/tmp/views-store-${process.pid}-${nextDatabaseId}.sqlite`;
  nextDatabaseId += 1;
  const open = () => {
    const database = new Database(filename, { create: true });
    database.run("PRAGMA foreign_keys=ON");
    migrateViewStore(database, 1);
    return {
      store: sqliteService(database),
      restart: () => {
        database.close(false);
        return Promise.resolve(open());
      },
      close: () => {
        database.close(false);
        return Promise.resolve();
      },
    };
  };
  return Promise.resolve(open());
};
viewStoreConformance("SQLite", factory);
