import { Array as Arr, Effect } from "effect";
import type { SqlClient } from "effect/sql";
import type { Backend } from "../backend.ts";
import type { Accumulator, Entry, IndexEntry, Store } from "../store.ts";
import { encodeRowKey } from "../runtime/key.ts";
import { decodeAcc, decodeRow } from "./codecs.ts";

/** All engine SQL is here. Every statement binds at most 100 values. */
export const migrate = Effect.fn("ViewsSql.migrate")(function* (sql: SqlClient.SqlClient) {
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_integrals (
    namespace TEXT NOT NULL, relation TEXT NOT NULL, row_key TEXT NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY (namespace, relation, row_key)) WITHOUT ROWID`;
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_arrangements (
    namespace TEXT NOT NULL, arrangement TEXT NOT NULL, index_key TEXT NOT NULL,
    sort_key TEXT NOT NULL, row_key TEXT NOT NULL,
    PRIMARY KEY (namespace, arrangement, index_key, sort_key, row_key)) WITHOUT ROWID`;
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_accumulators (
    namespace TEXT NOT NULL, node TEXT NOT NULL, group_key TEXT NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY (namespace, node, group_key)) WITHOUT ROWID`;
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_key_owners (
    namespace TEXT NOT NULL, relation TEXT NOT NULL, output_key TEXT NOT NULL, owner_key TEXT NOT NULL,
    PRIMARY KEY (namespace, relation, output_key)) WITHOUT ROWID`;
});

function storeFor(sql: SqlClient.SqlClient, namespace: string): Store {
  const get = (relation: string, rowKey: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{ value: string }>`SELECT value FROM streamsy_view_integrals
        WHERE namespace = ${namespace} AND relation = ${relation} AND row_key = ${rowKey}`;

      return rows[0] === undefined ? undefined : decodeRow(rows[0].value);
    });

  return {
    rows: (relation: string) =>
      Effect.map(
        sql<{
          value: string;
        }>`SELECT value FROM streamsy_view_integrals WHERE namespace = ${namespace} AND relation = ${relation}`,
        (rows) => Arr.map(rows, (row) => decodeRow(row.value)),
      ),
    snapshotMark: (arrangement: string, key: string) =>
      sql`INSERT INTO streamsy_view_arrangements
      (namespace, arrangement, index_key, sort_key, row_key) VALUES (${namespace}, ${arrangement}, ${""}, ${""}, ${key}) ON CONFLICT DO NOTHING`.pipe(
        Effect.asVoid,
      ),
    clearIndex: (arrangement: string) =>
      sql`DELETE FROM streamsy_view_arrangements WHERE namespace = ${namespace} AND arrangement = ${arrangement}`.pipe(
        Effect.asVoid,
      ),
    get,
    getMany: (relation, keys) =>
      Effect.gen(function* () {
        const result = new Map<string, Entry>();

        if (keys.length === 1) {
          const key = keys[0]!;
          const entry = yield* get(relation, key);

          if (entry !== undefined) result.set(key, entry);

          return result;
        }

        // Drive primary-key lookups directly from the values. Workerd counts
        // reads from SQLite's temporary IN table as billable reads.
        for (const batch of Arr.chunksOf(keys, 98)) {
          const values = Arr.map(batch, (key) => sql`(${key})`);

          const rows = yield* sql<{
            row_key: string;
            value: string;
          }>`WITH keys(key) AS (VALUES ${sql.csv(values)})
            SELECT i.row_key, i.value FROM keys CROSS JOIN streamsy_view_integrals AS i
            WHERE i.namespace = ${namespace} AND i.relation = ${relation} AND i.row_key = keys.key`;

          for (const row of rows) result.set(row.row_key, decodeRow(row.value));
        }

        return result;
      }),
    put: (relation: string, entries: readonly Entry[]) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(entries, 25)) {
          if (batch.length === 0) continue;

          const values = Arr.map(batch, (entry) => ({
            namespace,
            relation,
            row_key: encodeRowKey(entry.key),
            value: JSON.stringify(entry),
          }));

          yield* sql`INSERT INTO streamsy_view_integrals ${sql.insert(values)}
          ON CONFLICT (namespace, relation, row_key) DO UPDATE SET value = excluded.value`;
        }
      }),
    remove: (relation: string, keys: readonly string[]) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(keys, 98)) {
          if (batch.length === 0) continue;
          yield* sql`DELETE FROM streamsy_view_integrals WHERE namespace = ${namespace}
          AND relation = ${relation} AND ${sql.in("row_key", batch)}`;
        }
      }),
    indexPut: (arrangement: string, entries: readonly IndexEntry[]) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(entries, 20)) {
          if (batch.length === 0) continue;
          yield* sql`INSERT INTO streamsy_view_arrangements ${sql.insert(Arr.map(batch, (entry) => ({ namespace, arrangement, ...entry })))}`;
        }
      }),
    indexRemove: (arrangement: string, entries: readonly IndexEntry[]) =>
      Effect.gen(function* () {
        // Use the full five-column primary key for each point delete.
        for (const batch of Arr.chunksOf(entries, 20)) {
          const clauses = Arr.map(
            batch,
            (entry) => sql`(namespace = ${namespace} AND arrangement = ${arrangement}
              AND index_key = ${entry.index_key} AND sort_key = ${entry.sort_key} AND row_key = ${entry.row_key})`,
          );

          yield* sql`DELETE FROM streamsy_view_arrangements WHERE ${sql.or(clauses)}`;
        }
      }),
    lookup: (arrangement: string, indexKey: string, limit?: number, reverse = false) => {
      const base = sql`SELECT row_key, sort_key FROM streamsy_view_arrangements
        WHERE namespace = ${namespace} AND arrangement = ${arrangement} AND index_key = ${indexKey}`;

      const ordered = reverse
        ? sql`${base} ORDER BY sort_key DESC, row_key DESC`
        : sql`${base} ORDER BY sort_key, row_key`;

      return limit === undefined
        ? sql<{ row_key: string; sort_key: string }>`${ordered}`
        : sql<{ row_key: string; sort_key: string }>`${ordered} LIMIT ${limit}`;
    },
    accumulator: (node: string, groupKey: string) =>
      Effect.gen(function* () {
        const rows = yield* sql<{ value: string }>`SELECT value FROM streamsy_view_accumulators
        WHERE namespace = ${namespace} AND node = ${node} AND group_key = ${groupKey}`;

        return rows[0] === undefined ? undefined : decodeAcc(rows[0].value);
      }),
    accumulatorPut: (node: string, groupKey: string, value: Accumulator) =>
      sql`INSERT INTO streamsy_view_accumulators
      (namespace, node, group_key, value) VALUES (${namespace}, ${node}, ${groupKey}, ${JSON.stringify(value)})
      ON CONFLICT (namespace, node, group_key) DO UPDATE SET value = excluded.value`.pipe(
        Effect.asVoid,
      ),
    accumulatorRemove: (node: string, groupKey: string) =>
      sql`DELETE FROM streamsy_view_accumulators
      WHERE namespace = ${namespace} AND node = ${node} AND group_key = ${groupKey}`.pipe(
        Effect.asVoid,
      ),
    owners: (relation, keys) =>
      Effect.gen(function* () {
        const result = new Map<string, string>();

        for (const batch of Arr.chunksOf(keys, 98)) {
          const rows = yield* sql<{
            output_key: string;
            owner_key: string;
          }>`SELECT output_key, owner_key
            FROM streamsy_view_key_owners WHERE namespace = ${namespace} AND relation = ${relation}
            AND ${sql.in("output_key", batch)}`;

          for (const row of rows) result.set(row.output_key, row.owner_key);
        }

        return result;
      }),
    ownersPut: (relation, owners) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf([...owners], 25)) {
          const values = Arr.map(batch, ([output_key, owner_key]) => ({
            namespace,
            relation,
            output_key,
            owner_key,
          }));

          yield* sql`INSERT INTO streamsy_view_key_owners ${sql.insert(values)}
            ON CONFLICT (namespace, relation, output_key) DO UPDATE SET owner_key = excluded.owner_key`;
        }
      }),
    ownersRemove: (relation, keys) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(keys, 98))
          yield* sql`DELETE FROM streamsy_view_key_owners WHERE namespace = ${namespace}
            AND relation = ${relation} AND ${sql.in("output_key", batch)}`;
      }),
  };
}

/** Circuit metadata is separate from the operator tables. */
export const migrateCircuits = Effect.fn("ViewsSql.migrateCircuits")(function* (
  sql: SqlClient.SqlClient,
) {
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_circuits (
    namespace TEXT PRIMARY KEY, plan_hash TEXT NOT NULL) WITHOUT ROWID`;
});

const clearNamespace = Effect.fn("ViewsSql.clearNamespace")(function* (
  sql: SqlClient.SqlClient,
  namespace: string,
) {
  yield* sql`DELETE FROM streamsy_view_integrals WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_arrangements WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_accumulators WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_key_owners WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_circuits WHERE namespace = ${namespace}`;
});

export const sqliteBackend = (sql: SqlClient.SqlClient): Backend => ({
  store: (namespace) => storeFor(sql, namespace),
  circuit: (namespace) => ({
    load: sql<{ plan_hash: string }>`
    SELECT plan_hash FROM streamsy_view_circuits WHERE namespace = ${namespace}`.pipe(
      Effect.map(([previous]) => previous?.plan_hash),
    ),
    save: (hash) =>
      sql`
    INSERT INTO streamsy_view_circuits (namespace, plan_hash) VALUES (${namespace}, ${hash})`.pipe(
        Effect.asVoid,
      ),
  }),
  clear: (namespace) => clearNamespace(sql, namespace),
  atomic: sql.withTransaction,
});
