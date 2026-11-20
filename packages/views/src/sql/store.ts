import { Array as Arr, Effect, Schema } from "effect";
import { SqlClient, type SqlError } from "effect/sql";
import type { JsonObject, RowKey } from "../ir/contracts.ts";
import { encodeRowKey } from "../runtime/key.ts";
import { rowKeySchema } from "./data-fault.ts";

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
  readonly integerMass?: Readonly<Record<string, number>>;
}

const rowCodec = Schema.fromJsonString(
  Schema.Struct({
    key: rowKeySchema,
    row: Schema.JsonObject,
  }),
);

const accCodec = Schema.fromJsonString(
  Schema.Struct({
    count: Schema.Finite,
    sums: Schema.Record(Schema.String, Schema.Finite),
    integerMass: Schema.optionalKey(Schema.Record(Schema.String, Schema.Finite)),
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
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_key_owners (
    namespace TEXT NOT NULL, relation TEXT NOT NULL, output_key TEXT NOT NULL, owner_key TEXT NOT NULL,
    PRIMARY KEY (namespace, relation, output_key)) WITHOUT ROWID`;
});

type Query<A> = Effect.Effect<A, SqlError.SqlError>;

/** Namespace-local operator state. Keys passed to batch reads are encoded identities. */
export interface Store {
  rows(relation: string): Query<readonly Entry[]>;
  get(relation: string, key: string): Query<Entry | undefined>;
  getMany(relation: string, keys: readonly string[]): Query<ReadonlyMap<string, Entry>>;
  put(relation: string, entries: readonly Entry[]): Query<void>;
  remove(relation: string, keys: readonly string[]): Query<void>;
  snapshotMark(arrangement: string, key: string): Query<void>;
  clearIndex(arrangement: string): Query<void>;
  indexPut(arrangement: string, entries: readonly IndexEntry[]): Query<void>;
  indexRemove(arrangement: string, entries: readonly IndexEntry[]): Query<void>;
  lookup(
    arrangement: string,
    indexKey: string,
    limit?: number,
    reverse?: boolean,
  ): Query<readonly Pick<IndexEntry, "row_key" | "sort_key">[]>;
  accumulator(node: string, groupKey: string): Query<Accumulator | undefined>;
  accumulatorPut(node: string, groupKey: string, value: Accumulator): Query<void>;
  accumulatorRemove(node: string, groupKey: string): Query<void>;
  owners(relation: string, keys: readonly string[]): Query<ReadonlyMap<string, string>>;
  ownersPut(relation: string, owners: ReadonlyMap<string, string>): Query<void>;
  ownersRemove(relation: string, keys: readonly string[]): Query<void>;
}

export function storeFor(sql: SqlClient.SqlClient, namespace: string): Store {
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

        for (const batch of Arr.chunksOf(keys, 98)) {
          const rows = yield* sql<{ row_key: string; value: string }>`SELECT row_key, value
            FROM streamsy_view_integrals WHERE namespace = ${namespace} AND relation = ${relation}
            AND ${sql.in("row_key", batch)}`;

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
        if (entries.length === 1) {
          const entry = entries[0]!;
          yield* sql`DELETE FROM streamsy_view_arrangements
            WHERE namespace = ${namespace} AND arrangement = ${arrangement}
            AND index_key = ${entry.index_key} AND sort_key = ${entry.sort_key} AND row_key = ${entry.row_key}`;

          return;
        }

        // Full primary keys keep the delete on point lookups. The namespace and
        // arrangement consume two bindings, leaving 32 triples of entry values.
        for (const batch of Arr.chunksOf(entries, 32)) {
          const tuples = Arr.map(
            batch,
            (entry) => sql`(${entry.index_key}, ${entry.sort_key}, ${entry.row_key})`,
          );

          yield* sql`DELETE FROM streamsy_view_arrangements
            WHERE namespace = ${namespace} AND arrangement = ${arrangement}
            AND (index_key, sort_key, row_key) IN (VALUES ${sql.csv(tuples)})`;
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

/** R1 had no ownership or sum-membership indexes. Preserve any open State snapshot. */
export const legacyStore = (sql: SqlClient.SqlClient, namespace: string) => ({
  accumulators: (node: string) =>
    Effect.map(
      sql<{
        group_key: string;
        value: string;
      }>`SELECT group_key, value FROM streamsy_view_accumulators
        WHERE namespace = ${namespace} AND node = ${node}`,
      (rows) => new Map(Arr.map(rows, (row) => [row.group_key, decodeAcc(row.value)])),
    ),
  clearOperators: () =>
    Effect.gen(function* () {
      yield* sql`DELETE FROM streamsy_view_integrals WHERE namespace = ${namespace} AND relation <> '@snapshots'`;
      yield* sql`DELETE FROM streamsy_view_arrangements WHERE namespace = ${namespace} AND arrangement NOT LIKE '@snapshot/%'`;
      yield* sql`DELETE FROM streamsy_view_accumulators WHERE namespace = ${namespace}`;
    }),
});

/** Circuit metadata is separate from the operator tables. */
export const migrateCircuits = Effect.fn("ViewsSql.migrateCircuits")(function* (
  sql: SqlClient.SqlClient,
) {
  yield* sql`CREATE TABLE IF NOT EXISTS streamsy_view_circuits (
    namespace TEXT PRIMARY KEY, projection_key TEXT NOT NULL,
    generation INTEGER NOT NULL, plan_hash TEXT NOT NULL) WITHOUT ROWID`;
  yield* sql`CREATE INDEX IF NOT EXISTS streamsy_view_circuits_key
    ON streamsy_view_circuits (projection_key, generation)`;
});

export const clearNamespace = Effect.fn("ViewsSql.clearNamespace")(function* (
  sql: SqlClient.SqlClient,
  namespace: string,
) {
  yield* sql`DELETE FROM streamsy_view_integrals WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_arrangements WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_accumulators WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_key_owners WHERE namespace = ${namespace}`;
  yield* sql`DELETE FROM streamsy_view_circuits WHERE namespace = ${namespace}`;
});

export const circuitState = (sql: SqlClient.SqlClient, projectionKey: string) => ({
  generations: () => sql<{ namespace: string; generation: number; plan_hash: string }>`
    SELECT namespace, generation, plan_hash FROM streamsy_view_circuits
    WHERE projection_key = ${projectionKey}`,
  save: (namespace: string, generation: number, hash: string) => sql`
    INSERT INTO streamsy_view_circuits (namespace, projection_key, generation, plan_hash)
    VALUES (${namespace}, ${projectionKey}, ${generation}, ${hash})`,
});
