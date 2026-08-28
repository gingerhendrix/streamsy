/** Bun placement for the platform-neutral Effect SQL application store. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { OUTBOX_SCHEMA } from "@streamsy/effect-sink/sqlite";
import { importLegacyIssueStore, migrateViewStore } from "@streamsy/views-store/sqlite";
import { planHash } from "@streamsy/views";
import { Database } from "bun:sqlite";
import { Context, Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import { issueLifecycle, issues } from "../domain/declaration.ts";
import {
  APPLICATION_SCHEMA,
  COMMAND_RECEIPTS_SCHEMA,
  createSqliteIssueStoreBoundary,
  sqlLayer,
} from "./store-sql.ts";
import type { OutboxStore } from "@streamsy/effect-sink";
import type { IssueStore } from "./store.ts";

interface TableInfoRow {
  readonly name: string;
}

export interface SqliteStoreOptions {
  readonly filename: string;
}

function migrateCommandReceipts(database: Database): void {
  const columns = database.query<TableInfoRow, []>("PRAGMA table_info(command_receipts)").all();
  if (columns.some((column) => column.name === "request_hash")) return;
  database.transaction(() => {
    database.exec("DROP TABLE command_receipts");
    database.exec(COMMAND_RECEIPTS_SCHEMA);
  })();
}

export const prepareSqliteStore = (options: SqliteStoreOptions): void => {
  const database = new Database(options.filename, { create: true });
  try {
    database.exec("PRAGMA journal_mode = WAL");
    database.exec("PRAGMA foreign_keys = ON");
    database.exec(APPLICATION_SCHEMA);
    database.exec(OUTBOX_SCHEMA);
    migrateCommandReceipts(database);
    migrateViewStore(database);
    importLegacyIssueStore(database, {
      planName: issues.name,
      planHash: planHash(issues.plan),
      partition: "main",
      sourceId: "issue-tracker.issue-events",
      relationId: issues.name,
      reducerId: issueLifecycle.ref.name,
      reducerVersion: issueLifecycle.ref.version,
    });
  } finally {
    database.close(false);
  }
};

export const sqliteClientLayer = (options: SqliteStoreOptions) =>
  Layer.effectContext(
    Effect.gen(function* () {
      yield* Effect.sync(() => prepareSqliteStore(options));
      const client = yield* SqliteClient.make({ filename: options.filename, create: true });
      yield* client.unsafe<Record<string, never>>("PRAGMA foreign_keys = ON").pipe(Effect.asVoid);
      return Context.empty().pipe(
        Context.add(SqliteClient.SqliteClient, client),
        Context.add(SqlClient.SqlClient, client),
      );
    }),
  ).pipe(Layer.provide(Reactivity.layer));

export const sqliteLayer = (options: SqliteStoreOptions): Layer.Layer<IssueStore | OutboxStore> =>
  sqlLayer.pipe(Layer.provide(Layer.orDie(sqliteClientLayer(options))));

export { createSqliteIssueStoreBoundary };
