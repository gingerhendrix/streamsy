/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures and hooks. */
/**
 * The storage checks every server dialect runs: the core storage contract,
 * plus the SQL edge and migration cases that do not depend on SQLite files.
 * One `<dialect>.test.ts` file calls `storageDialectSuite` per dialect.
 */
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import {
  Array,
  Cause,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Order,
  Predicate,
  Scope,
  type Scope as ScopeType,
} from "effect";
import {
  Offset,
  ProducerId,
  Storage,
  StorageFault,
  StreamId,
  ZERO_OFFSET,
  type StreamRecord,
} from "@streamsy/core";
import { StorageContract } from "@streamsy/core/testing";
import { SqlClient } from "effect/sql";
import { migrate } from "../../src/migrations.ts";
import { STORAGE_SCHEMA_VERSION, STORAGE_VERSION_TABLE } from "../../src/schema.ts";
import { storageLayer } from "../../src/storage.ts";
import { dialectClient, freshDatabase, type SqlDialect } from "./support.ts";

const STORAGE_TABLES = [
  "streamsy_messages",
  "streamsy_owner_records",
  "streamsy_producers",
  "streamsy_streams",
] as const;

const died = <A, E>(exit: Exit.Exit<A, E>): boolean =>
  Exit.isFailure(exit) && Cause.hasDies(exit.cause);

const record = (id: string, expiresAtMs?: number): StreamRecord => ({
  id: StreamId.make(id),
  config: { contentType: "text/plain", createdAt: 0 },
  lifecycle:
    expiresAtMs === undefined
      ? { closed: false, softDeleted: false }
      : { closed: false, softDeleted: false, expiresAtMs },
  currentOffset: ZERO_OFFSET,
});

const one = Offset.make("0000000000000001_0000000000000000");

const encode = (text: string) => new TextEncoder().encode(text);

/** A producer write that expects `expected` and advances its sequence by one. */
const producer = (expected: Option.Option<{ epoch: number; lastSeq: number }>) => ({
  producerId: ProducerId.make("p"),
  expected,
  next: { epoch: 0, lastSeq: Option.isNone(expected) ? 0 : expected.value.lastSeq + 1 },
});

/** Postgres returns `COUNT(*)` as a bigint, MySQL as a number. */
const count = (sql: SqlClient.SqlClient, table: string) =>
  sql<{ readonly count: unknown }>`SELECT COUNT(*) AS count FROM ${sql(table)}`.pipe(
    Effect.map((rows) => Number(rows[0]?.count)),
  );

export const storageDialectSuite = (dialect: SqlDialect): void => {
  const baseUrl = dialect.url;

  if (baseUrl === undefined) {
    test.skip(`${dialect.name} storage suite (set its database URL to run)`, () => {});

    return;
  }

  // One database per test file. Each case starts from empty tables.
  let scope: Scope.Closeable | undefined;
  let url = "";
  beforeAll(async () => {
    scope = await Effect.runPromise(Scope.make());
    url = await Effect.runPromise(freshDatabase(dialect, baseUrl).pipe(Scope.provide(scope)));
  });
  afterAll(async () => {
    if (scope !== undefined) await Effect.runPromise(Scope.close(scope, Exit.void));
  });

  const reset = Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      for (const table of STORAGE_TABLES) yield* sql`DELETE FROM ${sql(table)}`;
    }),
  );

  const storeLayer = Layer.unwrap(
    Effect.sync(() =>
      reset.pipe(
        Layer.provideMerge(
          storageLayer({ repairIntervalMs: 1_000 }).pipe(
            Layer.provideMerge(dialectClient(dialect, url)),
          ),
        ),
      ),
    ),
  );

  StorageContract.run({
    name: `${dialect.name} Storage contract`,
    layer: storeLayer.pipe(Layer.orDie),
    expected: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    ownerRecords: true,
  });

  const withStore = <A, E>(
    use: (
      storage: typeof Storage.Service,
      sql: SqlClient.SqlClient,
    ) => Effect.Effect<A, E, ScopeType.Scope>,
  ) =>
    Effect.gen(function* () {
      const context = yield* Layer.build(storeLayer);

      return yield* use(Context.get(context, Storage), Context.get(context, SqlClient.SqlClient));
    }).pipe(Effect.scoped);

  /** A fresh database for cases that inspect or break the schema itself. */
  const withDatabase = <A, E>(use: (url: string) => Effect.Effect<A, E, ScopeType.Scope>) =>
    freshDatabase(dialect, baseUrl).pipe(
      Effect.flatMap((fresh) => Effect.scoped(use(fresh))),
      Effect.scoped,
    );

  describe(`${dialect.name} SQL edges`, () => {
    test("corrupt record and message rows defect at decoded edges", async () => {
      const result = await Effect.runPromise(
        withStore((storage, sql) =>
          Effect.gen(function* () {
            yield* storage.mutate({
              operations: [{ _tag: "Create", record: record("bad", 10), initialMessages: [] }],
            });
            yield* sql`UPDATE streamsy_streams SET current_offset = ${"invalid"} WHERE stream_id = ${"bad"}`;
            const badRecord = yield* storage.record(StreamId.make("bad")).pipe(Effect.exit);
            yield* sql`UPDATE streamsy_streams SET current_offset = ${ZERO_OFFSET} WHERE stream_id = ${"bad"}`;
            yield* sql`INSERT INTO streamsy_messages ${sql.insert({
              stream_id: "bad",
              msg_offset: "0000000000000001_invalid",
              timestamp: 1,
              data: new Uint8Array([1]),
            })}`;
            const badMessage = yield* storage.messages(StreamId.make("bad"), {}).pipe(Effect.exit);

            return [died(badRecord), died(badMessage)];
          }),
        ),
      );

      expect(result).toEqual([true, true]);
    });

    test("message SQL excludes rows outside the window and reads nothing for limit zero", async () => {
      const result = await Effect.runPromise(
        withStore((storage, sql) =>
          Effect.gen(function* () {
            yield* storage.mutate({
              operations: [
                {
                  _tag: "Create",
                  record: record("bounded"),
                  initialMessages: [{ offset: one, timestamp: 1, data: encode("included") }],
                },
              ],
            });
            // Sorts after `one` and fails the offset check when read.
            yield* sql`INSERT INTO streamsy_messages ${sql.insert({
              stream_id: "bounded",
              msg_offset: "0000000000000002_invalid",
              timestamp: 2,
              data: new Uint8Array([1]),
            })}`;
            const included = yield* storage.messages(StreamId.make("bounded"), { until: one });
            const zero = yield* storage.messages(StreamId.make("bounded"), { limit: 0 });
            const full = yield* storage.messages(StreamId.make("bounded"), {}).pipe(Effect.exit);

            return {
              included: Array.map(included, ({ offset }) => offset),
              zero,
              fullDefected: died(full),
            };
          }),
        ),
      );

      expect(result).toEqual({ included: [one], zero: [], fullDefected: true });
    });

    test("SQL failures map to StorageFault while concurrent producer CAS has one winner", async () => {
      const result = await Effect.runPromise(
        withDatabase((fresh) =>
          Effect.gen(function* () {
            const context = yield* Layer.build(
              storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
            );

            const storage = Context.get(context, Storage);
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* storage.mutate({
              operations: [{ _tag: "Create", record: record("race"), initialMessages: [] }],
            });
            yield* storage.mutate({
              operations: [
                {
                  _tag: "Append",
                  streamId: StreamId.make("race"),
                  messages: [],
                  patch: {},
                  producer: producer(Option.none()),
                },
              ],
            });

            const write = storage.mutate({
              operations: [
                {
                  _tag: "Append",
                  streamId: StreamId.make("race"),
                  messages: [],
                  patch: {},
                  producer: producer(Option.some({ epoch: 0, lastSeq: 0 })),
                },
              ],
            });

            const outcomes = yield* Effect.forEach(
              [write, write],
              (effect) => effect.pipe(Effect.catchTag("MutationRejected", Effect.succeed)),
              { concurrency: 2 },
            );

            yield* sql`ALTER TABLE streamsy_streams RENAME TO streamsy_streams_gone`;
            const fault = yield* storage.record(StreamId.make("race")).pipe(Effect.exit);

            return {
              tags: Array.sort(
                Array.map(outcomes, ({ _tag }) => _tag),
                Order.String,
              ),
              typedFault: Exit.isFailure(fault)
                ? !Cause.hasDies(fault.cause) &&
                  Option.match(Cause.findErrorOption(fault.cause), {
                    onNone: () => false,
                    onSome: Predicate.isTagged("StorageFault"),
                  })
                : false,
            };
          }),
        ),
      );

      expect(result).toEqual({ tags: ["Applied", "MutationRejected"], typedFault: true });
    });

    test("stream ids longer than 512 characters fail before any insert", async () => {
      const result = await Effect.runPromise(
        withStore((storage, sql) => {
          const create = (id: string) =>
            storage.mutate({
              operations: [{ _tag: "Create", record: record(id), initialMessages: [] }],
            });

          return Effect.gen(function* () {
            // 512 code points pass, even when they take 1024 UTF-16 units.
            yield* create("a".repeat(512));
            yield* create("\u{1F600}".repeat(512));
            const tooLong = yield* create("b".repeat(513)).pipe(Effect.flip);

            return {
              fault: Predicate.isTagged(tooLong, "StorageFault")
                ? { operation: tooLong.operation, retryable: tooLong.retryable }
                : tooLong,
              count: yield* count(sql, "streamsy_streams"),
            };
          });
        }),
      );

      expect(result).toEqual({
        fault: { operation: "mutate.stream-id", retryable: false },
        count: 2,
      });
    });

    test("producer ids longer than 255 characters fail before any write", async () => {
      const result = await Effect.runPromise(
        withStore((storage, sql) =>
          Effect.gen(function* () {
            yield* storage.mutate({
              operations: [{ _tag: "Create", record: record("producers"), initialMessages: [] }],
            });

            const write = (producerId: string) =>
              storage.mutate({
                operations: [
                  {
                    _tag: "Append",
                    streamId: StreamId.make("producers"),
                    messages: [{ offset: one, timestamp: 1, data: encode("x") }],
                    patch: { currentOffset: one },
                    producer: {
                      producerId: ProducerId.make(producerId),
                      expected: Option.none(),
                      next: { epoch: 0, lastSeq: 0 },
                    },
                  },
                ],
              });

            const tooLong = yield* write("p".repeat(256)).pipe(Effect.flip);

            const before = [
              yield* count(sql, "streamsy_producers"),
              yield* count(sql, "streamsy_messages"),
            ];

            yield* write("\u{1F600}".repeat(255));

            return {
              fault: Predicate.isTagged(tooLong, "StorageFault")
                ? { operation: tooLong.operation, retryable: tooLong.retryable }
                : tooLong,
              before,
              after: yield* count(sql, "streamsy_producers"),
            };
          }),
        ),
      );

      expect(result).toEqual({
        fault: { operation: "mutate.producer-id", retryable: false },
        before: [0, 0],
        after: 1,
      });
    });

    // A padding collation such as MySQL `utf8mb4_bin` would make "x" and "x " one key.
    test("ids that differ only by a trailing space are different streams and producers", async () => {
      const result = await Effect.runPromise(
        withStore((storage, sql) =>
          Effect.gen(function* () {
            const ids = ["x", "x "];

            for (const id of ids)
              yield* storage.mutate({
                operations: [{ _tag: "Create", record: record(id), initialMessages: [] }],
              });

            for (const [lastSeq, producerId] of ["p", "p "].entries())
              yield* storage.mutate({
                operations: [
                  {
                    _tag: "Append",
                    streamId: StreamId.make("x"),
                    messages: [],
                    patch: {},
                    producer: {
                      producerId: ProducerId.make(producerId),
                      expected: Option.none(),
                      next: { epoch: 0, lastSeq },
                    },
                  },
                ],
              });
            const records = yield* Effect.forEach(ids, (id) => storage.record(StreamId.make(id)));

            const producers = yield* Effect.forEach(["p", "p "], (id) =>
              storage.producer(StreamId.make("x"), ProducerId.make(id)),
            );

            return {
              ids: Array.map(
                records,
                Option.map(({ id }) => id),
              ),
              seqs: Array.map(
                producers,
                Option.map(({ lastSeq }) => lastSeq),
              ),
              counts: [
                yield* count(sql, "streamsy_streams"),
                yield* count(sql, "streamsy_producers"),
              ],
            };
          }),
        ),
      );

      expect(result).toEqual({
        ids: [Option.some(StreamId.make("x")), Option.some(StreamId.make("x "))],
        seqs: [Option.some(0), Option.some(1)],
        counts: [2, 2],
      });
    });
  });

  const tableNames = (sql: SqlClient.SqlClient) =>
    sql
      .unsafe<{ readonly name: string }>(dialect.tables)
      .pipe(Effect.map((rows) => Array.map(rows, ({ name }) => name)));

  describe(`${dialect.name} migrations`, () => {
    test("reopens a nonempty current-format database", async () => {
      const result = await Effect.runPromise(
        withDatabase((fresh) =>
          Effect.gen(function* () {
            // Migrate, then close the first layer before reopening.
            yield* Layer.build(
              storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
            ).pipe(Effect.scoped);

            const writer = Context.get(
              yield* Layer.build(dialectClient(dialect, fresh)),
              SqlClient.SqlClient,
            );

            yield* writer`INSERT INTO streamsy_streams ${writer.insert({
              stream_id: "kept",
              content_type: "text/plain",
              created_at: 0,
              current_offset: ZERO_OFFSET,
              closed: 0,
              soft_deleted: 0,
              expires_at_ms: 42,
            })}`;

            const reopened = Context.get(
              yield* Layer.build(
                storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
              ),
              Storage,
            );

            return yield* reopened.nextExpiry;
          }),
        ),
      );

      expect(result).toMatchObject({ _tag: "Some", value: { at: 42, streamId: "kept" } });
    });

    test("an injected migration failure records no version and a rerun completes", async () => {
      const result = await Effect.runPromise(
        withDatabase((fresh) =>
          Effect.gen(function* () {
            const sql = Context.get(
              yield* Layer.build(dialectClient(dialect, fresh)),
              SqlClient.SqlClient,
            );

            const failed = yield* migrate(sql, {
              beforeVersion: (version) =>
                version === 1
                  ? Effect.fail(
                      new StorageFault({
                        operation: "migration.injected",
                        message: "injected",
                        retryable: false,
                      }),
                    )
                  : Effect.void,
            }).pipe(Effect.exit);

            const tables = yield* tableNames(sql);

            const versions = tables.includes(STORAGE_VERSION_TABLE)
              ? yield* count(sql, STORAGE_VERSION_TABLE)
              : 0;

            yield* migrate(sql);

            return {
              failed: Exit.isFailure(failed),
              tables,
              versions,
              rerun: yield* tableNames(sql),
            };
          }),
        ),
      );

      expect(result).toEqual({
        failed: true,
        // MySQL commits DDL by itself, so only its version table outlives the failure.
        tables: dialect.name === "mysql" ? [STORAGE_VERSION_TABLE] : [],
        versions: 0,
        rerun: Array.sort([...STORAGE_TABLES, STORAGE_VERSION_TABLE], Order.String),
      });
    });

    test("legacy and too-new schemas reject without changes", async () => {
      const outcomes: globalThis.Array<unknown> = [];

      for (const { kind, table, version } of [
        { kind: "legacy", table: "streamsy_schema_version", version: 3 },
        { kind: "legacy-0.4", table: "streamsy_storage_schema_version", version: 2 },
        { kind: "newer", table: STORAGE_VERSION_TABLE, version: STORAGE_SCHEMA_VERSION + 1 },
      ]) {
        outcomes.push(
          await Effect.runPromise(
            withDatabase((fresh) =>
              Effect.gen(function* () {
                const sql = Context.get(
                  yield* Layer.build(dialectClient(dialect, fresh)),
                  SqlClient.SqlClient,
                );

                yield* sql`CREATE TABLE ${sql(table)} (version INTEGER PRIMARY KEY, applied_at_ms BIGINT NOT NULL)`;
                yield* sql`INSERT INTO ${sql(table)} ${sql.insert({ version, applied_at_ms: 1 })}`;
                const exit = yield* migrate(sql).pipe(Effect.exit);

                const fault = Exit.isFailure(exit)
                  ? Option.getOrUndefined(Cause.findErrorOption(exit.cause))
                  : undefined;

                return {
                  kind,
                  operation: fault?.operation,
                  retryable: fault?.retryable,
                  tables: yield* tableNames(sql),
                };
              }),
            ),
          ),
        );
      }

      const rejected = { operation: "migration.open", retryable: false };
      expect(outcomes).toEqual([
        { kind: "legacy", ...rejected, tables: ["streamsy_schema_version"] },
        { kind: "legacy-0.4", ...rejected, tables: ["streamsy_storage_schema_version"] },
        { kind: "newer", ...rejected, tables: [STORAGE_VERSION_TABLE] },
      ]);
    });

    test("concurrent initialization exposes a complete schema", async () => {
      const result = await Effect.runPromise(
        withDatabase((fresh) =>
          Effect.gen(function* () {
            const open = Layer.build(
              storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
            ).pipe(Effect.map((context) => Context.get(context, Storage)));

            const [left, right] = yield* Effect.all([open, open], { concurrency: 2 });

            const sql = Context.get(
              yield* Layer.build(dialectClient(dialect, fresh)),
              SqlClient.SqlClient,
            );

            return {
              left: yield* left.record(StreamId.make("x")),
              right: yield* right.record(StreamId.make("x")),
              versions: Array.map(
                yield* sql<{
                  readonly version: unknown;
                }>`SELECT version FROM ${sql(STORAGE_VERSION_TABLE)}`,
                ({ version }) => Number(version),
              ),
            };
          }),
        ),
      );

      expect(result).toEqual({
        left: Option.none(),
        right: Option.none(),
        versions: [STORAGE_SCHEMA_VERSION],
      });
    });
  });
};
