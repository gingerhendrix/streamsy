import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import { viewStoreConformance } from "./conformance.ts";
import { migrateViewStore } from "./sqlite-schema.ts";
import { sqliteService } from "./sqlite.ts";

let nextDatabaseId = 0;

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

const openStore = (filename: string) => {
  const runtime = ManagedRuntime.make(sqliteClientLayer(filename));
  const store = runtime.runSync(Effect.map(SqlClient.SqlClient, sqliteService));
  return {
    store,
    restart: () => runtime.dispose().then(() => openStore(filename)),
    close: () => runtime.dispose(),
  };
};

const factory = () => {
  const filename = `/tmp/views-store-${process.pid}-${nextDatabaseId}.sqlite`;
  nextDatabaseId += 1;
  const database = new Database(filename, { create: true });
  database.run("PRAGMA foreign_keys=ON");
  migrateViewStore(database, 1);
  database.close(false);
  return Promise.resolve(openStore(filename));
};

viewStoreConformance("SQLite", factory);
