import { Array as Arr, Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import type { JsonObject, RowKey } from "../ir/contracts.ts";
import { encodeRowKey } from "../runtime/key.ts";

export interface Entry {
  readonly key: RowKey;
  readonly row: JsonObject;
}

export interface IndexEntry {
  readonly index_key: string;
  readonly sort_key: string;
  readonly row_key: string;
}

export interface Accumulator {
  readonly count: number;
  readonly sums: Readonly<Record<string, number>>;
}

const rowCodec = Schema.fromJsonString(
  Schema.Struct({
    key: Schema.Union([
      Schema.String,
      Schema.Finite,
      Schema.Boolean,
      Schema.Array(Schema.Union([Schema.String, Schema.Finite, Schema.Boolean])),
    ]),
    row: Schema.JsonObject,
  }),
);

const accCodec = Schema.fromJsonString(
  Schema.Struct({
    count: Schema.Finite,
    sums: Schema.Record(Schema.String, Schema.Finite),
  }),
);

const decodeRow = Schema.decodeUnknownSync(rowCodec);

const decodeAcc = Schema.decodeUnknownSync(accCodec);

/** All engine SQL is here. Every statement binds at most 100 values. */
export const migrate = Effect.fn("ViewsSql.migrate")(function* () {
  const sql = yield* SqlClient.SqlClient;
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
});

export function storeFor(sql: SqlClient.SqlClient, namespace: string) {
  return {
    get: (relation: string, rowKey: string) =>
      Effect.gen(function* () {
        const rows = yield* sql<{ value: string }>`SELECT value FROM streamsy_view_integrals
        WHERE namespace = ${namespace} AND relation = ${relation} AND row_key = ${rowKey}`;

        return rows[0] === undefined ? undefined : decodeRow(rows[0].value);
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
        for (const entry of entries)
          yield* sql`DELETE FROM streamsy_view_arrangements
        WHERE namespace = ${namespace} AND arrangement = ${arrangement}
        AND index_key = ${entry.index_key} AND sort_key = ${entry.sort_key} AND row_key = ${entry.row_key}`;
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
    accumulatorPut: (
      node: string,
      groupKey: string,
      value: Accumulator,
    ) => sql`INSERT INTO streamsy_view_accumulators
      (namespace, node, group_key, value) VALUES (${namespace}, ${node}, ${groupKey}, ${JSON.stringify(value)})
      ON CONFLICT (namespace, node, group_key) DO UPDATE SET value = excluded.value`,
    accumulatorRemove: (
      node: string,
      groupKey: string,
    ) => sql`DELETE FROM streamsy_view_accumulators
      WHERE namespace = ${namespace} AND node = ${node} AND group_key = ${groupKey}`,
  };
}

export type Store = ReturnType<typeof storeFor>;
