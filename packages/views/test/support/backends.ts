import { SqliteClient } from "@effect/sql-sqlite-bun";
import type { Streamsy } from "@streamsy/core";
import { CheckpointStore, OwnerTransaction, StateTable, Transaction } from "@streamsy/projection";
import { SqlStorage } from "@streamsy/storage";
import { BunStorage } from "@streamsy/storage/bun";
import { Context, Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import {
  dialectClient,
  freshDatabase,
  sqlDialects,
  type SqlDialect,
} from "../../../storage/test/dialects/support.ts";
import { View, ViewBackend } from "../../src/index.ts";
import { backendFor } from "../../src/sql/backend.ts";

export interface SqlFixture {
  readonly name: string;
  readonly server?: SqlDialect;
}

export const sqlite: SqlFixture = { name: "SQLite" };

export const postgres = { name: "Postgres", server: sqlDialects.pg } satisfies SqlFixture;

export const fixtureDatabase = (server: SqlDialect) =>
  server.url === undefined
    ? Effect.die(new Error(`Missing test URL for ${server.name}`))
    : freshDatabase(server, server.url);

const fixtureClient = (server: SqlDialect) =>
  Layer.unwrap(Effect.map(fixtureDatabase(server), (url) => dialectClient(server, url)));

export const client = (fixture: SqlFixture) =>
  fixture.server === undefined
    ? SqliteClient.layer({ filename: ":memory:" })
    : fixtureClient(fixture.server);

const commits = Layer.mergeAll(
  CheckpointStore.layer,
  OwnerTransaction.layer,
  ViewBackend.layerSql,
  Transaction.layer,
);

const sqlHost = <const Keys extends readonly Streamsy.Any[]>(group: Streamsy.Group<Keys>) =>
  commits.pipe(Layer.provideMerge(SqlStorage.layer(group, { readLimit: 200 })));

export const hostAt = <const Keys extends readonly Streamsy.Any[]>(
  server: SqlDialect,
  group: Streamsy.Group<Keys>,
  url: string,
) => sqlHost(group).pipe(Layer.provideMerge(dialectClient(server, url)));

export const hostFor = <const Keys extends readonly Streamsy.Any[]>(
  fixture: SqlFixture,
  group: Streamsy.Group<Keys>,
) =>
  fixture.server === undefined
    ? commits.pipe(
        Layer.provideMerge(
          BunStorage.layer(group, { client: { filename: ":memory:" }, readLimit: 200 }),
        ),
      )
    : sqlHost(group).pipe(Layer.provideMerge(fixtureClient(fixture.server)));

export const createBackend = (fixture: SqlFixture) =>
  Effect.gen(function* () {
    const sql = Context.get(yield* Layer.build(client(fixture)), SqlClient.SqlClient);
    yield* View.migrate(sql);

    return yield* backendFor(sql);
  });

/** Provide tables for the output assertions that use StateTable's row reader. */
export const migrateTestReader = (sql: SqlClient.SqlClient) =>
  sql.onDialectOrElse({
    pg: () =>
      Effect.gen(function* () {
        yield* sql`CREATE TABLE IF NOT EXISTS streamsy_state_heads (
          stream_id TEXT PRIMARY KEY, applied_offset TEXT NOT NULL,
          epoch INTEGER NOT NULL DEFAULT 0, snapshot_open INTEGER NOT NULL DEFAULT 0)`;
        yield* sql`CREATE TABLE IF NOT EXISTS streamsy_state_rows (
          stream_id TEXT NOT NULL, collection TEXT NOT NULL, row_key TEXT COLLATE "C" NOT NULL,
          value TEXT NOT NULL, epoch INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (stream_id, collection, row_key))`;
      }),
    orElse: () => StateTable.migrate(sql),
  });
