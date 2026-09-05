/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- bun:test owns the Promise-native two-client interleaving harness and its explicit pause/resume barriers. */
/* oxlint-disable typescript/no-unsafe-type-assertion -- The query hook preserves the complete SqlClient and unsafe signatures while tapping only the selected test query's Effect. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import { viewStoreConformance } from "./conformance.ts";
import type { MaintenanceCommit, NamespaceRef, ViewIdentity } from "./contracts.ts";
import { migrateViewStore } from "./sqlite-schema.ts";
import { sqliteService } from "./sqlite.ts";

let nextDatabaseId = 0;

const sqliteClientLayer = (filename: string, readonly = false) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* SqliteClient.make(
        readonly ? { filename, readonly: true } : { filename, create: true },
      );
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

const identity: ViewIdentity = {
  planName: "snapshot-interleaving",
  planHash: "hash-1",
  partition: "main",
  sourceId: "source",
};
const relation: NamespaceRef = { ...identity, id: "rows" };
const commit = (
  cursor: string,
  expectedCursor: string | undefined,
  suffix: Partial<MaintenanceCommit> = {},
): MaintenanceCommit => ({
  identity,
  expectedCursor,
  afterExclusiveCursor: cursor,
  batchId: `batch-${cursor}`,
  committedAtMs: Number(cursor),
  ...suffix,
});

const interleavedSql = (
  sql: SqlClient.SqlClient,
  afterQuery: string,
  reached: () => void,
  resume: Promise<void>,
): SqlClient.SqlClient => {
  let paused = false;
  // SAFETY: the proxy preserves every client member and replaces `unsafe` with
  // the same call signature while only tapping its returned Effect in this test.
  return {
    ...sql,
    unsafe: (<A extends object>(statement: string, params?: ReadonlyArray<unknown>) => {
      const result = sql.unsafe<A>(statement, params);
      if (paused || !statement.includes(afterQuery)) return result;
      paused = true;
      return result.pipe(
        Effect.tap(() =>
          Effect.promise(() => {
            reached();
            return resume;
          }),
        ),
      );
    }) as SqlClient.SqlClient["unsafe"],
  } as SqlClient.SqlClient;
};

const withInterleavedStores = async <A>(
  afterQuery: string,
  setup: (store: ReturnType<typeof sqliteService>) => Promise<void>,
  read: (store: ReturnType<typeof sqliteService>) => Promise<A>,
  write: (store: ReturnType<typeof sqliteService>, sql: SqlClient.SqlClient) => Promise<void>,
): Promise<A> => {
  const filename = `/tmp/views-store-interleaving-${process.pid}-${nextDatabaseId}.sqlite`;
  nextDatabaseId += 1;
  const database = new Database(filename, { create: true });
  database.run("PRAGMA journal_mode=WAL");
  database.run("PRAGMA foreign_keys=ON");
  migrateViewStore(database, 1);
  database.close(false);

  const readerRuntime = ManagedRuntime.make(sqliteClientLayer(filename, true));
  const writerRuntime = ManagedRuntime.make(sqliteClientLayer(filename));
  const readerSql = readerRuntime.runSync(SqlClient.SqlClient);
  const writerSql = writerRuntime.runSync(SqlClient.SqlClient);
  let markReached!: () => void;
  let resumeReader!: () => void;
  const reached = new Promise<void>((resolve) => {
    markReached = resolve;
  });
  const resume = new Promise<void>((resolve) => {
    resumeReader = resolve;
  });
  const readerStore = sqliteService(interleavedSql(readerSql, afterQuery, markReached, resume));
  const writerStore = sqliteService(writerSql);
  try {
    await setup(writerStore);
    const reading = read(readerStore);
    await reached;
    await write(writerStore, writerSql);
    resumeReader();
    return await reading;
  } finally {
    resumeReader();
    await readerRuntime.dispose();
    await writerRuntime.dispose();
  }
};

test("snapshotRows reads its cursor and rows from one committed revision", async () => {
  const result = await withInterleavedStores(
    "SELECT p.source_cursor,v.value_key,v.value_json",
    (store) =>
      Effect.runPromise(
        store.commit(
          commit("1", undefined, {
            rows: [{ kind: "put", namespace: relation, key: "issue", value: { revision: 1 } }],
          }),
        ),
      ).then(() => undefined),
    (store) => Effect.runPromise(store.snapshotRows(relation)),
    (store) =>
      Effect.runPromise(
        store.commit(
          commit("2", "1", {
            rows: [{ kind: "put", namespace: relation, key: "issue", value: { revision: 2 } }],
          }),
        ),
      ).then(() => undefined),
  );
  expect(result).toEqual({ sourceCursor: "1", rows: [{ key: "issue", value: { revision: 1 } }] });
});

test("historyBounds reads its retained range and epoch from one committed revision", async () => {
  const result = await withInterleavedStores(
    "SELECT MIN(history_seq) first,MAX(history_seq) latest",
    (store) => Effect.runPromise(store.commit(commit("1", undefined))).then(() => undefined),
    (store) => Effect.runPromise(store.historyBounds(identity)),
    (_store, sql) =>
      Effect.runPromise(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("DELETE FROM streamsy_view_change_batches");
            yield* sql.unsafe(
              "UPDATE streamsy_view_partitions SET history_epoch=2,history_floor=2,next_history_seq=3",
            );
            yield* sql.unsafe(
              "INSERT INTO streamsy_view_change_batches VALUES (?,?,?,?,?,?,?,?,?)",
              [
                identity.planName,
                identity.partition,
                2,
                2,
                identity.sourceId,
                "2",
                "batch-2",
                identity.planHash,
                2,
              ],
            );
          }),
        ),
      ),
  );
  expect(result).toEqual({ epoch: 1, first: 1, latest: 1 });
});

test("changesAfter keeps a selected batch and its change rows in one snapshot", async () => {
  const result = await withInterleavedStores(
    "WITH p AS (SELECT",
    (store) =>
      Effect.runPromise(
        store.commit(
          commit("1", undefined, {
            changes: [
              { kind: "enter", relationId: relation.id, key: "issue", after: { revision: 1 } },
            ],
          }),
        ),
      ).then(() => undefined),
    (store) => Effect.runPromise(store.changesAfter(identity, undefined, 10)),
    (store) =>
      Effect.runPromise(store.commit(commit("2", "1"), { keepLastBatches: 1 })).then(
        () => undefined,
      ),
  );
  expect(result[0]).toMatchObject({
    position: { epoch: 1, sequence: 1 },
    sourceCursor: "1",
    changes: [{ kind: "enter", relationId: "rows", key: "issue", after: { revision: 1 } }],
  });
});

test("loadCheckpoint keeps an active manifest and its entries in one snapshot", async () => {
  const descriptor = { ...identity, reducerId: "reducer", reducerVersion: 1 };
  const result = await withInterleavedStores(
    "WITH target AS (SELECT generation,source_cursor,created_at_ms,entry_count",
    (store) =>
      Effect.runPromise(
        store.saveCheckpoint({
          ...descriptor,
          sourceCursor: "1",
          createdAtMs: 1,
          entries: [{ key: "issue", value: { revision: 1 } }],
        }),
      ).then(() => undefined),
    (store) => Effect.runPromise(store.loadCheckpoint(descriptor)),
    (store) =>
      Effect.runPromise(
        store.saveCheckpoint({
          ...descriptor,
          sourceCursor: "2",
          createdAtMs: 2,
          entries: [{ key: "issue", value: { revision: 2 } }],
          keepGenerations: 1,
        }),
      ).then(() => undefined),
  );
  expect(result).toMatchObject({
    generation: 1,
    sourceCursor: "1",
    entries: [{ key: "issue", value: { revision: 1 } }],
  });
});
