/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/* oxlint-disable effecttsgo/strict-effect-provide -- Each test provides its host within the fresh database scope. */
/**
 * Checkpoints on the storage owner (`CheckpointStore.layer`) and fused commits on
 * Postgres and MySQL, plus runner concurrency on all three SQL dialects. Server
 * dialects use fresh databases and skip when their URL is absent.
 */
import { describe, expect, test } from "vitest";
import { Array, Context, Deferred, Effect, Layer, Option, Order, Result, Schema } from "effect";
import { SqlClient, SqlError } from "effect/sql";
import { Storage, StorageFault, StreamId, Streamsy } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import { SqlStorage } from "@streamsy/storage";
import type { CheckpointRecord } from "@streamsy/projection/checkpoint";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  ProjectionWakeup,
  Transaction,
} from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { initialize, input, output, readAll } from "./support/scenarios.ts";
import {
  dialectClient,
  freshDatabase,
  sqlDialects,
  type SqlDialect,
} from "../../storage/test/dialects/support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];

const TIMEOUT_MS = 60_000;

const SAVES = 8;

const key = (id: string) => ({ id, version: 1, generation: 1, params: { tenant: "ä/1" } });

/**
 * A wake key with a 482-digit hex folder id, like the longest artifacts-fs ids. Its
 * checkpoint key has 551 characters, and two suffixes share the first 545.
 */
const folder = (suffix: string) => ({
  id: "files-wake",
  version: 1,
  generation: 1,
  params: { folderId: `${"6461696c792f".repeat(40)}${suffix}` },
});

const record = (state: string): CheckpointRecord => ({
  identity: { inputs: {} },
  inputs: {},
  adapters: {},
  state,
});

/** Run `use` on a fresh database of `dialect`. */
const onFreshDatabase = <A, E>(
  dialect: SqlDialect,
  baseUrl: string,
  use: (url: string) => Effect.Effect<A, E, import("effect").Scope.Scope>,
) =>
  Effect.runPromise(
    freshDatabase(dialect, baseUrl).pipe(
      Effect.flatMap((url) => use(url).pipe(Effect.scoped)),
      Effect.scoped,
    ),
  );

/** Checkpoints on the storage owner, over one pool. */
const checkpoints = (dialect: SqlDialect, url: string) =>
  CheckpointStore.layer.pipe(
    Layer.provideMerge(SqlStorage.layerProtocol()),
    Layer.provideMerge(dialectClient(dialect, url)),
  );

/** Projection checkpoints and transactions joined to storage, as one host process over its own pool. */
const host = (dialect: SqlDialect, url: string) =>
  Layer.build(
    FamilyFixture.withFamilies(
      Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
        Layer.provideMerge(SqlStorage.layerProtocol()),
        Layer.provideMerge(dialectClient(dialect, url)),
      ),
    ),
  );

const count = (sql: SqlClient.SqlClient) =>
  sql<{ readonly n: unknown }>`SELECT COUNT(*) AS n FROM seen`.pipe(
    Effect.map((rows) => Number(rows[0]?.n)),
  );

/** Fails at the checkpoint save, after the handler's SQL and append ran. */
const failingCheckpoints = Effect.map(CheckpointStore, (owner) =>
  CheckpointStore.of({
    ...owner,
    compareAndSet: () =>
      Effect.fail(
        new ProjectionFault({ phase: "checkpoint", reason: "storage-failure", message: "forced" }),
      ),
  }),
);

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} projection SQL`, () => {
    test(
      "the checkpoint store reads, writes, overwrites and removes",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* Layer.build(checkpoints(dialect, fresh));
            // A second build finds the schema migrated and creates nothing.
            yield* Layer.build(checkpoints(dialect, fresh));
            const store = Context.get(context, CheckpointStore);
            const sql = Context.get(context, SqlClient.SqlClient);
            const absent = yield* store.load(key("upper"));
            const first = yield* store.compareAndSet(key("upper"), "0", record("one"));
            const second = yield* store.compareAndSet(key("upper"), first, record("two"));
            // The key column compares bytes, so a key that differs only in case is another row.
            yield* store.compareAndSet(key("UPPER"), "0", record("other"));
            const loaded = yield* store.load(key("upper"));

            const stale = yield* store
              .compareAndSet(key("upper"), first, record("stale"))
              .pipe(Effect.result);

            const rows = yield* sql<{
              readonly owner_key: string;
              readonly owner_value: string;
            }>`SELECT owner_key, owner_value FROM streamsy_owner_records`;

            yield* store.remove(key("upper"));
            // Removing an absent key is ignored.
            yield* store.remove(key("upper"));

            return {
              absent: absent.token,
              tokens: [first, second],
              loaded: [loaded.token, Option.map(loaded.record, ({ state }) => state)],
              stale: Result.isFailure(stale) ? stale.failure.reason : "applied",
              rows: rows.length,
              json: Array.every(rows, ({ owner_value }) => owner_value.startsWith("{")),
              removed: (yield* store.load(key("upper"))).token,
              other: Option.map((yield* store.load(key("UPPER"))).record, ({ state }) => state),
            };
          }),
        );

        expect(result).toEqual({
          absent: "0",
          tokens: ["1", "2"],
          loaded: ["2", Option.some("two")],
          stale: "token-conflict",
          rows: 2,
          json: true,
          removed: "0",
          other: Option.some("other"),
        });
      },
      TIMEOUT_MS,
    );

    test(
      "long checkpoint keys stay distinct and only reversible keys can be listed",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const store = Context.get(
              yield* Layer.build(checkpoints(dialect, fresh)),
              CheckpointStore,
            );

            const first = yield* store.compareAndSet(folder("01"), "0", record("one"));
            yield* store.compareAndSet(folder("01"), first, record("two"));
            yield* store.compareAndSet(folder("02"), "0", record("other"));

            const saved = (suffix: string) =>
              Effect.map(store.load(folder(suffix)), (loaded) => [
                loaded.token,
                Option.map(loaded.record, (found) => found.state),
              ]);

            const bindings = yield* Projection.bindings({
              id: folder("01").id,
              params: { folderId: Schema.String },
            }).pipe(Effect.provideService(CheckpointStore, store), Effect.result);

            return { saved: [yield* saved("01"), yield* saved("02")], bindings };
          }),
        );

        expect(result.saved).toEqual([
          ["2", Option.some("two")],
          ["1", Option.some("other")],
        ]);

        if (dialect.name === "mysql") {
          expect(Result.getFailure(result.bindings)).toMatchObject(
            Option.some({
              reason: "unsupported-composition",
              message: expect.stringContaining("hashed"),
            }),
          );
        } else {
          const params = Option.getOrThrow(Result.getSuccess(result.bindings));
          expect(params).toHaveLength(2);
          expect(params).toEqual(
            expect.arrayContaining([folder("01").params, folder("02").params]),
          );
        }
      },
      TIMEOUT_MS,
    );

    test(
      "concurrent saves of one key from two pools have one winner",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            // Two separate layer builds, so the saves race across two pools.
            const first = Context.get(
              yield* Layer.build(checkpoints(dialect, fresh)),
              CheckpointStore,
            );

            const second = Context.get(
              yield* Layer.build(checkpoints(dialect, fresh)),
              CheckpointStore,
            );

            const stores = [first, second];

            /** SAVES saves of `id` from `ifToken`, alternating pools, all at once. */
            const race = (id: string, ifToken: string) =>
              Effect.all(
                Array.makeBy(SAVES, (index) =>
                  stores[index % 2]!.compareAndSet(key(id), ifToken, record(`save-${index}`)).pipe(
                    Effect.map((token) => ({ index, outcome: `saved ${token}` })),
                    Effect.catchTag("ProjectionFault", (fault) =>
                      Effect.succeed({ index, outcome: fault.reason }),
                    ),
                  ),
                ),
                { concurrency: "unbounded" },
              ).pipe(
                Effect.flatMap((outcomes) =>
                  Effect.map(first.load(key(id)), (loaded) => {
                    const winners = Array.filter(outcomes, ({ outcome }) =>
                      outcome.startsWith("saved"),
                    );

                    return {
                      outcomes: Array.sort(
                        Array.map(outcomes, ({ outcome }) => outcome),
                        Order.String,
                      ),
                      // The stored record is the winner's, not a later loser's.
                      stored:
                        winners.length === 1 &&
                        Option.getOrUndefined(loaded.record)?.state === `save-${winners[0]!.index}`,
                      token: loaded.token,
                    };
                  }),
                ),
              );

            yield* first.compareAndSet(key("existing"), "0", record("seed"));

            return { existing: yield* race("existing", "1"), absent: yield* race("absent", "0") };
          }),
        );

        const conflicts = Array.replicate("token-conflict", SAVES - 1);
        expect(result).toEqual({
          existing: {
            outcomes: Array.sort(["saved 2", ...conflicts], Order.String),
            stored: true,
            token: "2",
          },
          absent: {
            outcomes: Array.sort(["saved 1", ...conflicts], Order.String),
            stored: true,
            token: "1",
          },
        });
      },
      TIMEOUT_MS,
    );

    test(
      "checkpoint keys that differ only by a trailing space are different rows",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* Layer.build(checkpoints(dialect, fresh));
            const sql = Context.get(context, SqlClient.SqlClient);

            // Encoded keys end in "]", so the column is checked directly.
            for (const owner_key of ["x", "x "])
              yield* sql`INSERT INTO streamsy_owner_records ${sql.insert({
                owner_key,
                owner_value: "{}",
              })}`;

            const rows = yield* sql<{
              readonly owner_key: string;
            }>`SELECT owner_key FROM streamsy_owner_records WHERE owner_key = ${"x"}`;

            return Array.map(rows, ({ owner_key }) => owner_key);
          }),
        );

        expect(result).toEqual(["x"]);
      },
      TIMEOUT_MS,
    );

    test(
      "a fused commit writes the app row, the output append and the checkpoint together",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const context = yield* host(dialect, fresh);

            return yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`CREATE TABLE seen (item INTEGER NOT NULL)`;
              yield* initialize;

              const projection = Projection.process({
                commit: Commit.transaction,
                id: "fused",
                params: {},
                inputs: { input },
                process: (batch) =>
                  Effect.gen(function* () {
                    for (const item of batch.input.items)
                      yield* sql`INSERT INTO seen ${sql.insert({ item })}`;
                    const kept = Array.filter(batch.input.items, (item) => item > 0);

                    if (Array.isReadonlyArrayNonEmpty(kept)) yield* Streams.append(output, kept);
                  }),
              }).at({});

              const owner = yield* CheckpointStore;

              const failed = yield* Projection.run(projection).pipe(
                Effect.provideService(CheckpointStore, yield* failingCheckpoints),
                Effect.result,
              );

              const afterFailure = {
                failed: Result.isFailure(failed),
                seen: yield* count(sql),
                output: yield* readAll(output),
                token: (yield* owner.load(Projection.key(projection))).token,
              };

              const run = yield* Projection.run(projection);

              return {
                afterFailure,
                status: run.status,
                seen: yield* count(sql),
                output: yield* readAll(output),
                token: (yield* owner.load(Projection.key(projection))).token,
              };
            }).pipe(Effect.provide(context));
          }),
        );

        expect(result).toEqual({
          afterFailure: { failed: true, seen: 0, output: [], token: "0" },
          status: "caught-up",
          seen: 3,
          output: [1, 3],
          token: "1",
        });
      },
      TIMEOUT_MS,
    );

    test(
      "a fused commit appends to its output after another pool appended to it",
      async () => {
        if (url === undefined) return;

        const result = await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            // Build one host first, so the second finds the schema migrated.
            const context = yield* host(dialect, fresh);
            const other = yield* host(dialect, fresh);
            const otherWriter = Context.get(other, StreamsWriter);

            return yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`CREATE TABLE seen (item INTEGER NOT NULL)`;
              yield* initialize;
              let interleaved = false;

              const projection = Projection.process({
                commit: Commit.transaction,
                id: "fused-race",
                params: {},
                inputs: { input },
                process: (batch) =>
                  Effect.gen(function* () {
                    // On MySQL this plain read fixes the transaction's snapshot.
                    yield* count(sql);

                    if (!interleaved) {
                      interleaved = true;
                      // Another process appends to the output after that read.
                      yield* FamilyFixture.provideWriter(otherWriter)(
                        Streams.append(output, [100]),
                      );
                    }

                    for (const item of batch.input.items)
                      yield* sql`INSERT INTO seen ${sql.insert({ item })}`;
                    const kept = Array.filter(batch.input.items, (item) => item > 0);

                    if (Array.isReadonlyArrayNonEmpty(kept)) yield* Streams.append(output, kept);
                  }),
              }).at({});

              const run = Result.match(yield* Projection.run(projection).pipe(Effect.result), {
                onSuccess: ({ status }) => status,
                onFailure: (error) => `failed:${String(error)}`,
              });

              const storage = yield* Storage;
              const id = StreamId.make(output.id);
              const messages = yield* storage.messages(id, {});
              const offsets = Array.map(messages, ({ offset }) => offset);
              const tail = Option.map(yield* storage.record(id), (stored) => stored.currentOffset);

              return {
                run,
                seen: yield* count(sql),
                output: yield* readAll(output),
                ordered: Array.every(
                  offsets,
                  (offset, index) => index === 0 || offsets[index - 1]! < offset,
                ),
                tail: Option.getOrUndefined(tail) === offsets.at(-1),
              };
            }).pipe(Effect.provide(context));
          }),
        );

        expect(result).toEqual({
          run: "caught-up",
          seen: 3,
          output: [100, 1, 3],
          ordered: true,
          tail: true,
        });
      },
      TIMEOUT_MS,
    );
  });
}

class ConcurrentSource extends Streamsy.json<ConcurrentSource>()(
  "SqlConcurrency/Source",
  "concurrent/:id",
  {
    params: { id: Schema.String },
    schema: Schema.Finite,
  },
) {}

/** One runner over the same client and transaction owner as its input and outputs. */
const concurrentHost = <A, E, R>(
  base: Layer.Layer<A | StreamsWriter | import("@streamsy/core/protocol").StreamsReader, E, R>,
) =>
  FamilyFixture.withFamilies(
    Layer.mergeAll(CheckpointStore.layer, Transaction.layer, Projection.Registry.layer).pipe(
      Layer.provideMerge(base),
    ),
    ConcurrentSource,
  );

const concurrentUnits = (overlap: boolean) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE concurrent_seen (id VARCHAR(10) PRIMARY KEY)`;
    yield* Streams.create(output);
    const both = yield* Deferred.make<void>();
    let active = 0;
    let maximum = 0;

    const declaration = Projection.process({
      id: "concurrent-units",
      params: { id: Schema.String },
      inputs: { source: ConcurrentSource },
      commit: Commit.transaction,
      process: (_batch, unit) =>
        Effect.gen(function* () {
          const txSql = yield* SqlClient.SqlClient;
          active += 1;
          maximum = Math.max(maximum, active);

          if (active === 2) yield* Deferred.succeed(both, undefined);
          // Parallel dialects must meet at the latch. Serial drivers yield while holding their permit.
          yield* overlap ? Deferred.await(both) : Effect.sleep("20 millis");
          yield* txSql`INSERT INTO concurrent_seen ${txSql.insert({ id: unit.params.id })}`;
          yield* Streams.append(output, [Number(unit.params.id)]);
          active -= 1;
        }),
    });

    const wakeup = yield* ProjectionWakeup.attach(declaration, { concurrency: 2 });
    const source = yield* ConcurrentSource;

    for (const id of ["1", "2"]) {
      yield* source.create({ id });
      yield* source.append({ id }, [1]);
    }

    expect(yield* wakeup.run).toEqual({ more: false });
    const rows = yield* sql<{ id: string }>`SELECT id FROM concurrent_seen ORDER BY id`;
    expect(Array.map(rows, ({ id }) => id)).toEqual(["1", "2"]);
    expect(Array.sort([...(yield* readAll(output))], Order.Number)).toEqual([1, 2]);
    expect(maximum).toBe(overlap ? 2 : 1);
    const store = yield* CheckpointStore;

    for (const id of ["1", "2"]) {
      expect((yield* store.load(Projection.key(declaration.at({ id })))).token).toBe("1");
    }

    expect(yield* (yield* Projection.Registry).failed(declaration)).toEqual([]);
  });

test(
  "SQLite concurrent transaction bindings serialize their handler bodies and both commit",
  () =>
    Effect.runPromise(
      concurrentUnits(false).pipe(
        Effect.provide(
          concurrentHost(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
        ),
        Effect.scoped,
      ),
    ),
  TIMEOUT_MS,
);

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} parallel bindings`, () => {
    for (const pool of [10, 1]) {
      test(
        `transaction bindings both commit with pool size ${pool}`,
        async () => {
          if (url === undefined) return;
          await onFreshDatabase(dialect, url, (fresh) =>
            concurrentUnits(pool > 1).pipe(
              Effect.provide(
                concurrentHost(
                  SqlStorage.layerProtocol().pipe(
                    Layer.provideMerge(dialectClient(dialect, fresh, { maxConnections: pool })),
                  ),
                ),
              ),
            ),
          );
        },
        TIMEOUT_MS,
      );
    }

    test(
      "opposite stream lock orders back off the aborted binding and both eventually commit",
      async () => {
        if (url === undefined) return;
        await onFreshDatabase(dialect, url, (fresh) =>
          Effect.gen(function* () {
            const left = FamilyFixture.json("lock-left", { schema: Schema.Finite });
            const right = FamilyFixture.json("lock-right", { schema: Schema.Finite });
            yield* Streams.create(left);
            yield* Streams.create(right);
            const bothLocked = yield* Deferred.make<void>();
            let locked = 0;
            const deadlocks: boolean[] = [];
            const attempts = new Map<string, number>();

            const declaration = Projection.process({
              id: "deadlock-units",
              params: { id: Schema.String },
              inputs: { source: ConcurrentSource },
              commit: Commit.transaction,
              process: (_batch, unit) =>
                Effect.gen(function* () {
                  const attempt = (attempts.get(unit.params.id) ?? 0) + 1;
                  attempts.set(unit.params.id, attempt);
                  const streams = unit.params.id === "1" ? [left, right] : [right, left];
                  yield* Streams.append(streams[0]!, [Number(unit.params.id)]);

                  if (attempt === 1) {
                    locked += 1;

                    if (locked === 2) yield* Deferred.succeed(bothLocked, undefined);
                    yield* Deferred.await(bothLocked);
                  }

                  yield* Streams.append(streams[1]!, [Number(unit.params.id)]).pipe(
                    Effect.tapError((fault) =>
                      Effect.sync(() => {
                        deadlocks.push(
                          Schema.is(StorageFault)(fault) &&
                            SqlError.isSqlError(fault.cause) &&
                            Schema.is(SqlError.DeadlockError)(fault.cause.reason),
                        );
                      }),
                    ),
                  );
                }),
            });

            const wakeup = yield* ProjectionWakeup.attach(declaration, { concurrency: 2 });
            const source = yield* ConcurrentSource;

            for (const id of ["1", "2"]) {
              yield* source.create({ id });
              yield* source.append({ id }, [1]);
            }

            yield* wakeup.run;
            const registry = yield* Projection.Registry;
            const failed = yield* registry.failed(declaration);
            expect(deadlocks).toEqual([true]);
            expect(failed).toHaveLength(1);
            expect(failed[0]).toMatchObject({ attempts: 1, stopped: false });
            expect(Option.isSome(failed[0]!.dueAt)).toBe(true);
            // Direct handler appends expose a retryable StorageFault, which has no ProjectionFault reason.
            expect(Option.getOrThrow(failed[0]!.failure).reason).toEqual(Option.none());
            // Let the binding's recorded backoff elapse; the successful peer must not rerun.
            yield* Effect.sleep("1100 millis");
            yield* wakeup.run;
            expect(yield* registry.failed(declaration)).toEqual([]);
            expect(Array.sort([...attempts.values()], Order.Number)).toEqual([1, 2]);

            for (const stream of [left, right]) {
              expect(Array.sort([...(yield* readAll(stream))], Order.Number)).toEqual([1, 2]);
            }

            const store = yield* CheckpointStore;

            for (const id of ["1", "2"]) {
              expect((yield* store.load(Projection.key(declaration.at({ id })))).token).toBe("1");
            }
          }).pipe(
            Effect.provide(
              concurrentHost(
                SqlStorage.layerProtocol().pipe(Layer.provideMerge(dialectClient(dialect, fresh))),
              ),
            ),
          ),
        );
      },
      TIMEOUT_MS,
    );
  });
}
