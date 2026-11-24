/* oxlint-disable effecttsgo/prefer-schema-over-json -- Persist the same JSON text and property order as SQLite. */
import { NonRetryable } from "@streamsy/projection";
import { Array as Arr, Effect } from "effect";
import type { SqlClient, SqlError } from "effect/sql";
import type { Backend } from "../backend.ts";
import type { RowKey } from "../ir/contracts.ts";
import { compareRowKeys, encodeRowKey } from "../runtime/key.ts";
import type { Entry, IndexEntry, Store } from "../store.ts";
import { ViewFault } from "../view-fault.ts";
import { decodeAcc, decodeRow } from "./codecs.ts";
import { dataFault } from "./data-fault.ts";

const layoutVersion = 1;

const snapshotPrefix = "@snapshot/";

const idLimit = 255;

const prefixLength = 1024;

const encoder = new TextEncoder();

const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

type Bytes = Uint8Array<ArrayBuffer>;

type Migration = Effect.Effect<void, SqlError.SqlError | ViewFault>;

export interface ServerDialect {
  readonly ddl: readonly string[];
  readonly upsert: (keys: readonly string[], updates: readonly string[]) => string;
  readonly doNothing: string;
  readonly layoutExists?: (sql: SqlClient.SqlClient) => Effect.Effect<boolean, SqlError.SqlError>;
  readonly withMigrationLock: (sql: SqlClient.SqlClient, migrate: Migration) => Migration;
}

export interface ServerOptions {
  readonly prefixLength?: number;
  readonly onTieRepair?: () => void;
}

const digest = (bytes: Bytes) => globalThis.crypto.subtle.digest("SHA-256", bytes);

const hashBytes = (bytes: Bytes) =>
  Effect.promise(() => digest(bytes)).pipe(Effect.map((buffer) => new Uint8Array(buffer)));

const hash = (key: string) => hashBytes(encoder.encode(key));

const idFault = (id: string) => {
  const source = id.startsWith(snapshotPrefix) ? id.slice(snapshotPrefix.length) : id;

  return dataFault({ view: "*", node: source })(
    source,
    `Views identifier for ${JSON.stringify(source)} exceeds ${idLimit} UTF-8 bytes`,
  );
};

function idBytes(id: string) {
  const bytes = encoder.encode(id);

  return bytes.length > idLimit ? Effect.fail(idFault(id)) : Effect.succeed(bytes);
}

function keyBytes(node: string, key: string, original: RowKey = key) {
  if (!key.isWellFormed())
    return Effect.fail(
      dataFault({ view: "*", node })(
        original,
        `Key ${JSON.stringify(original)} contains a lone surrogate`,
      ),
    );

  return Effect.succeed(encoder.encode(key));
}

const hashKeys = (node: string, keys: readonly string[]) =>
  Effect.forEach(keys, (key) => hashKey(node, key), { concurrency: "unbounded" });

const hashKey = (node: string, key: string) => Effect.flatMap(keyBytes(node, key), hashBytes);

function ordPrefix(sortKey: string, row: Bytes, limit: number): Bytes {
  // sort_key is ASCII hex. Leave a zero between it and the UTF-8 row key.
  const sort = encoder.encode(sortKey.slice(0, limit));
  const prefix = new Uint8Array(Math.min(limit, sort.length + 1 + row.length));
  prefix.set(sort);

  if (sort.length < limit) prefix.set(row.subarray(0, limit - sort.length - 1), sort.length + 1);

  return prefix;
}

interface ArrangementRow {
  readonly row_key: Bytes;
  readonly sort_key: string;
  readonly ord_prefix: Bytes;
}

const sameBytes = (a: Bytes, b: Bytes) =>
  a.length === b.length && a.every((byte, i) => byte === b[i]);

const readLayoutVersion = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const [stored] = yield* sql<{
      version: number;
    }>`SELECT version FROM streamsy_view_layout WHERE id = 1`;

    if (stored !== undefined && stored.version > layoutVersion)
      return yield* Object.assign(
        new ViewFault({
          message: `Unsupported Views layout version ${stored.version}. Supported version is ${layoutVersion}`,
        }),
        { [NonRetryable]: true },
      );

    return stored?.version;
  });

export const migrateServer = (sql: SqlClient.SqlClient, dialect: ServerDialect): Migration =>
  dialect.withMigrationLock(
    sql,
    Effect.gen(function* () {
      // With autocommitting DDL, reject newer layouts before any write.
      let stored: number | undefined;

      if (dialect.layoutExists !== undefined && (yield* dialect.layoutExists(sql)))
        stored = yield* readLayoutVersion(sql);

      for (const statement of dialect.ddl) yield* sql.unsafe(statement);

      if (dialect.layoutExists === undefined) stored = yield* readLayoutVersion(sql);

      if (stored === undefined)
        yield* sql`INSERT INTO streamsy_view_layout (id, version) VALUES (1, ${layoutVersion})`;

      return undefined;
    }),
  );

function storeFor(
  sql: SqlClient.SqlClient,
  namespaceHash: Effect.Effect<Bytes>,
  dialect: ServerDialect,
  options: ServerOptions,
): Store {
  const length = options.prefixLength ?? prefixLength;

  const integralUpsert = sql.literal(
    dialect.upsert(["ns", "relation", "row_hash"], ["row_key", "value"]),
  );

  const accumulatorUpsert = sql.literal(
    dialect.upsert(["ns", "node", "group_hash"], ["group_key", "value"]),
  );

  const ownerUpsert = sql.literal(
    dialect.upsert(["ns", "relation", "output_hash"], ["output_key", "owner_key"]),
  );

  const doNothing = sql.literal(dialect.doNothing);

  const arrangementEntry = (arrangement: string, entry: IndexEntry) =>
    Effect.gen(function* () {
      const row_key = yield* keyBytes(arrangement, entry.row_key);

      const [index_hash, row_hash] = yield* Effect.all(
        [hash(entry.index_key), hashBytes(row_key)],
        { concurrency: "unbounded" },
      );

      return {
        index_hash,
        ord_prefix: ordPrefix(entry.sort_key, row_key, length),
        row_hash,
        sort_key: entry.sort_key,
        row_key,
      };
    });

  return {
    rows: (relation) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;

        const rows = yield* sql<{ value: string }>`SELECT value FROM streamsy_view_integrals
          WHERE ns = ${ns} AND relation = ${yield* idBytes(relation)}`;

        return Arr.map(rows, ({ value }) => decodeRow(value));
      }),
    get: (relation, key) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        const rowHash = yield* hashKey(relation, key);

        const [row] = yield* sql<{ value: string }>`SELECT value FROM streamsy_view_integrals
          WHERE ns = ${ns} AND relation = ${yield* idBytes(relation)} AND row_hash = ${rowHash}`;

        return row === undefined ? undefined : decodeRow(row.value);
      }),
    getMany: (relation, keys) =>
      Effect.gen(function* () {
        const result = new Map<string, Entry>();

        for (const batch of Arr.chunksOf(keys, 98)) {
          const ns = yield* namespaceHash;
          const hashes = yield* hashKeys(relation, batch);

          const rows = yield* sql<{ row_key: Bytes; value: string }>`SELECT row_key, value
            FROM streamsy_view_integrals WHERE ns = ${ns} AND relation = ${yield* idBytes(relation)}
            AND ${sql.in("row_hash", hashes)}`;

          for (const { row_key, value } of rows)
            result.set(decoder.decode(row_key), decodeRow(value));
        }

        return result;
      }),
    put: (relation, entries) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(entries, 20)) {
          const ns = yield* namespaceHash;
          const id = yield* idBytes(relation);
          // When the same key appears twice within one insert, keep its final value.
          const keyed = new Map(Arr.map(batch, (entry) => [encodeRowKey(entry.key), entry]));

          const values = yield* Effect.forEach(
            [...keyed],
            ([key, entry]) =>
              Effect.gen(function* () {
                const row_key = yield* keyBytes(relation, key, entry.key);
                const row_hash = yield* hashBytes(row_key);

                return { ns, relation: id, row_hash, row_key, value: JSON.stringify(entry) };
              }),
            { concurrency: "unbounded" },
          );

          if (Arr.isArrayNonEmpty(values))
            yield* sql`INSERT INTO streamsy_view_integrals ${sql.insert(values)} ${integralUpsert}`;
        }
      }),
    remove: (relation, keys) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(keys, 98)) {
          const ns = yield* namespaceHash;
          const hashes = yield* hashKeys(relation, batch);
          yield* sql`DELETE FROM streamsy_view_integrals WHERE ns = ${ns}
            AND relation = ${yield* idBytes(relation)} AND ${sql.in("row_hash", hashes)}`;
        }
      }),
    snapshotMark: (arrangement, key) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        const id = yield* idBytes(arrangement);

        const entry = yield* arrangementEntry(arrangement, {
          index_key: "",
          sort_key: "",
          row_key: key,
        });

        yield* sql`INSERT INTO streamsy_view_arrangements
          ${sql.insert({ ns, arrangement: id, ...entry })} ${doNothing}`;
      }),
    clearIndex: (arrangement) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        yield* sql`DELETE FROM streamsy_view_arrangements WHERE ns = ${ns}
          AND arrangement = ${yield* idBytes(arrangement)}`;
      }),
    indexPut: (arrangement, entries) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(entries, 14)) {
          const ns = yield* namespaceHash;
          const id = yield* idBytes(arrangement);

          const encoded = yield* Effect.forEach(
            batch,
            (entry) => arrangementEntry(arrangement, entry),
            {
              concurrency: "unbounded",
            },
          );

          const values = Arr.map(encoded, (entry) => ({ ns, arrangement: id, ...entry }));
          yield* sql`INSERT INTO streamsy_view_arrangements ${sql.insert(values)}`;
        }
      }),
    indexRemove: (arrangement, entries) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(entries, 20)) {
          const ns = yield* namespaceHash;
          const id = yield* idBytes(arrangement);

          const encoded = yield* Effect.forEach(
            batch,
            (entry) => arrangementEntry(arrangement, entry),
            {
              concurrency: "unbounded",
            },
          );

          const clauses = Arr.map(
            encoded,
            (entry) => sql`(ns = ${ns} AND arrangement = ${id} AND index_hash = ${entry.index_hash}
              AND ord_prefix = ${entry.ord_prefix} AND row_hash = ${entry.row_hash})`,
          );

          yield* sql`DELETE FROM streamsy_view_arrangements WHERE ${sql.or(clauses)}`;
        }
      }),
    lookup: (arrangement, indexKey, limit, reverse = false) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        const indexHash = yield* hash(indexKey);

        const base = sql`SELECT row_key, sort_key, ord_prefix FROM streamsy_view_arrangements
          WHERE ns = ${ns} AND arrangement = ${yield* idBytes(arrangement)} AND index_hash = ${indexHash}`;

        const ordered = reverse
          ? sql`${base} ORDER BY ord_prefix DESC`
          : sql`${base} ORDER BY ord_prefix`;

        let rows =
          limit === undefined
            ? yield* sql<ArrangementRow>`${ordered}`
            : yield* sql<ArrangementRow>`${ordered} LIMIT ${limit}`;

        const last = rows.at(-1);

        if (limit !== undefined && rows.length === limit && last?.ord_prefix.length === length) {
          options.onTieRepair?.();
          const tied = yield* sql<ArrangementRow>`${base} AND ord_prefix = ${last.ord_prefix}`;
          rows = [
            ...Arr.filter(rows, (row) => !sameBytes(row.ord_prefix, last.ord_prefix)),
            ...tied,
          ];
        }

        const decoded = Arr.map(rows, ({ row_key, sort_key }) => ({
          row_key: decoder.decode(row_key),
          sort_key,
        }));

        decoded.sort((a, b) => {
          const order =
            a.sort_key < b.sort_key
              ? -1
              : a.sort_key > b.sort_key
                ? 1
                : compareRowKeys(a.row_key, b.row_key);

          return reverse ? -order : order;
        });

        return limit === undefined ? decoded : decoded.slice(0, limit);
      }),
    accumulator: (node, groupKey) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        const groupHash = yield* hash(groupKey);

        const [row] = yield* sql<{ value: string }>`SELECT value FROM streamsy_view_accumulators
          WHERE ns = ${ns} AND node = ${yield* idBytes(node)} AND group_hash = ${groupHash}`;

        return row === undefined ? undefined : decodeAcc(row.value);
      }),
    accumulatorPut: (node, groupKey, value) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        const groupHash = yield* hash(groupKey);
        yield* sql`INSERT INTO streamsy_view_accumulators
          (ns, node, group_hash, group_key, value) VALUES
          (${ns}, ${yield* idBytes(node)}, ${groupHash}, ${groupKey}, ${JSON.stringify(value)}) ${accumulatorUpsert}`;
      }),
    accumulatorRemove: (node, groupKey) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash;
        const groupHash = yield* hash(groupKey);
        yield* sql`DELETE FROM streamsy_view_accumulators
          WHERE ns = ${ns} AND node = ${yield* idBytes(node)} AND group_hash = ${groupHash}`;
      }),
    owners: (relation, keys) =>
      Effect.gen(function* () {
        const result = new Map<string, string>();

        for (const batch of Arr.chunksOf(keys, 98)) {
          const ns = yield* namespaceHash;
          const hashes = yield* hashKeys(relation, batch);

          const rows = yield* sql<{
            output_key: Bytes;
            owner_key: Bytes;
          }>`SELECT output_key, owner_key
            FROM streamsy_view_key_owners WHERE ns = ${ns} AND relation = ${yield* idBytes(relation)}
            AND ${sql.in("output_hash", hashes)}`;

          for (const { output_key, owner_key } of rows)
            result.set(decoder.decode(output_key), decoder.decode(owner_key));
        }

        return result;
      }),
    ownersPut: (relation, owners) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf([...owners], 20)) {
          const ns = yield* namespaceHash;
          const id = yield* idBytes(relation);

          const values = yield* Effect.forEach(
            batch,
            ([key, owner]) =>
              Effect.gen(function* () {
                const output_key = yield* keyBytes(relation, key);
                const owner_key = yield* keyBytes(relation, owner);
                const output_hash = yield* hashBytes(output_key);

                return { ns, relation: id, output_hash, output_key, owner_key };
              }),
            { concurrency: "unbounded" },
          );

          yield* sql`INSERT INTO streamsy_view_key_owners ${sql.insert(values)} ${ownerUpsert}`;
        }
      }),
    ownersRemove: (relation, keys) =>
      Effect.gen(function* () {
        for (const batch of Arr.chunksOf(keys, 98)) {
          const ns = yield* namespaceHash;
          const hashes = yield* hashKeys(relation, batch);
          yield* sql`DELETE FROM streamsy_view_key_owners WHERE ns = ${ns}
            AND relation = ${yield* idBytes(relation)} AND ${sql.in("output_hash", hashes)}`;
        }
      }),
  };
}

export function serverBackend(
  sql: SqlClient.SqlClient,
  dialect: ServerDialect,
  options: ServerOptions = {},
): Backend {
  // A circuit gets one backend per unit. Share the namespace digest across its
  // guard, normalization and engine work.
  const hashes = new Map<string, Promise<ArrayBuffer>>();

  const namespaceHash = (namespace: string) =>
    Effect.promise(() => {
      const cached = hashes.get(namespace);

      if (cached !== undefined) return cached;
      const pending = digest(encoder.encode(namespace));
      hashes.set(namespace, pending);

      return pending;
    }).pipe(Effect.map((buffer) => new Uint8Array(buffer)));

  return {
    validateSource: (source) => {
      const id = snapshotPrefix + source;

      return encoder.encode(id).length > idLimit ? idFault(id) : undefined;
    },
    store: (namespace) => storeFor(sql, namespaceHash(namespace), dialect, options),
    circuit: (namespace) => ({
      load: Effect.gen(function* () {
        const ns = yield* namespaceHash(namespace);

        const [row] = yield* sql<{ plan_hash: string }>`SELECT plan_hash FROM streamsy_view_circuits
          WHERE ns = ${ns}`;

        return row?.plan_hash;
      }),
      save: (planHash) =>
        Effect.gen(function* () {
          const ns = yield* namespaceHash(namespace);
          yield* sql`INSERT INTO streamsy_view_circuits (ns, namespace, plan_hash)
            VALUES (${ns}, ${namespace}, ${planHash})`;
        }),
    }),
    clear: (namespace) =>
      Effect.gen(function* () {
        const ns = yield* namespaceHash(namespace);
        yield* sql`DELETE FROM streamsy_view_integrals WHERE ns = ${ns}`;
        yield* sql`DELETE FROM streamsy_view_arrangements WHERE ns = ${ns}`;
        yield* sql`DELETE FROM streamsy_view_accumulators WHERE ns = ${ns}`;
        yield* sql`DELETE FROM streamsy_view_key_owners WHERE ns = ${ns}`;
        yield* sql`DELETE FROM streamsy_view_circuits WHERE ns = ${ns}`;
      }),
    atomic: sql.withTransaction,
  };
}
