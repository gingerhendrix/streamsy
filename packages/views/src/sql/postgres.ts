import { Array, Effect } from "effect";
import type { ServerDialect } from "./server.ts";

// "strmview" as a 64-bit integer. Keep it out of JavaScript's Number range.
const migrationLock = "8319400226123769207";

export const postgres: ServerDialect = {
  ddl: [
    `CREATE TABLE IF NOT EXISTS streamsy_view_layout (
      id SMALLINT PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_circuits (
      ns BYTEA PRIMARY KEY, namespace TEXT NOT NULL, plan_hash TEXT NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_integrals (
      ns BYTEA NOT NULL, relation BYTEA NOT NULL, row_hash BYTEA NOT NULL,
      row_key BYTEA NOT NULL, value TEXT NOT NULL,
      PRIMARY KEY (ns, relation, row_hash))`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_arrangements (
      ns BYTEA NOT NULL, arrangement BYTEA NOT NULL, index_hash BYTEA NOT NULL,
      ord_prefix BYTEA NOT NULL, row_hash BYTEA NOT NULL,
      sort_key TEXT NOT NULL, row_key BYTEA NOT NULL,
      PRIMARY KEY (ns, arrangement, index_hash, ord_prefix, row_hash))`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_accumulators (
      ns BYTEA NOT NULL, node BYTEA NOT NULL, group_hash BYTEA NOT NULL,
      group_key TEXT NOT NULL, value TEXT NOT NULL,
      PRIMARY KEY (ns, node, group_hash))`,
    `CREATE TABLE IF NOT EXISTS streamsy_view_key_owners (
      ns BYTEA NOT NULL, relation BYTEA NOT NULL, output_hash BYTEA NOT NULL,
      output_key BYTEA NOT NULL, owner_key BYTEA NOT NULL,
      PRIMARY KEY (ns, relation, output_hash))`,
  ],
  upsert: (keys, updates) =>
    `ON CONFLICT (${Array.join(keys, ", ")}) DO UPDATE SET ${Array.join(
      Array.map(updates, (key) => `${key} = excluded.${key}`),
      ", ",
    )}`,
  doNothing: "ON CONFLICT DO NOTHING",
  withMigrationLock: (sql, migrate) =>
    sql.withTransaction(
      sql`SELECT pg_advisory_xact_lock(${sql.literal(migrationLock)})`.pipe(
        Effect.andThen(migrate),
      ),
    ),
};
