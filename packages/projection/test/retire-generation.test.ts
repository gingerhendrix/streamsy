import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Context, Deferred, Effect, Fiber, Layer, Option, Ref, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { StreamBusy, ZERO_OFFSET } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import {
  CheckpointRecord,
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  ProjectionWakeup,
  StateTable,
} from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { Counter } from "../src/unit.ts";
import { withFamilies } from "./support/families.ts";
import {
  Accepted,
  Facts,
  Numbers,
  Rows,
  declaration,
  numbers,
  output,
  sums,
  table,
} from "./support/generation-outputs.ts";
import { layerSqlite } from "./support/sqlite.ts";

const host = withFamilies(layerSqlite(), Facts, Numbers, Rows, Accepted);

const seed = (params: { readonly workspace: string }) =>
  Effect.gen(function* () {
    const input = yield* Facts;

    yield* input.create(params);
    yield* input.append(params, [{ id: "a", n: 2 }]);
  });

const binding = (workspace: string) => {
  const params = { workspace };
  const rev = { ...params, rev: 1 };
  const epoch = { ...params, epoch: 1 };

  return { params, rev, epoch, numberId: Numbers.at(rev).id, acceptedId: Accepted.at(epoch).id };
};

const retired = { phase: "load", reason: "retired-generation", retryable: false };

const missing = { _tag: "StreamNotFound" };

const unsupported = { phase: "checkpoint", reason: "unsupported-composition" };

const previousEnvelope = Schema.fromJsonString(
  Schema.Struct({ version: Counter, record: CheckpointRecord }),
);

layer(host)((it) => {
  it.effect("producer retirement overwrites an undecodable checkpoint and removes its output", () =>
    Effect.gen(function* () {
      const { params, rev, numberId } = binding("producer-undecodable");
      const spec = numbers(Commit.producer, 1);
      const bound = spec.at(params);
      const store = yield* CheckpointStore;
      const records = yield* OwnerRecords;
      const slot = recordKey(bound);
      yield* seed(params);
      yield* Projection.run(bound);
      yield* records.write(slot, '{"version":1,"record":{"future":true}}');
      expect(yield* store.load(bound).pipe(Effect.flip)).toMatchObject({
        reason: "invalid-record",
      });

      const result = yield* Projection.retire(spec, params, { generation: 1 });
      expect(result).toMatchObject({
        record: "retired",
        streams: expect.arrayContaining([{ output: "numbers", id: numberId, result: "removed" }]),
      });
      expect(yield* records.read(slot)).toEqual(Option.some('{"version":1,"retired":{}}'));
      const target = yield* Numbers;
      expect(yield* target.head(rev).pipe(Effect.flip)).toMatchObject(missing);
    }),
  );

  it.effect("transactionWithCleanup rolls back forget and callback SQL on failure", () =>
    Effect.gen(function* () {
      const { params } = binding("forget-with-cleanup");
      const store = yield* CheckpointStore;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE forget_cleanup (id TEXT NOT NULL)`;

      const failure = new ProjectionFault({
        phase: "checkpoint",
        reason: "storage-failure",
        message: "forget cleanup failed",
      });

      const commit = Commit.transactionWithCleanup((key) =>
        Effect.gen(function* () {
          expect((yield* store.load(key)).token).toBe("0");
          const client = yield* SqlClient.SqlClient;
          yield* client`INSERT INTO forget_cleanup (id) VALUES (${key.id})`.pipe(Effect.orDie);

          return yield* failure;
        }),
      );

      const bound = numbers(commit, 1).at(params);
      yield* seed(params);
      yield* Projection.run(bound);
      const checkpoint = yield* store.load(bound);
      expect(Option.isSome(checkpoint.record)).toBe(true);

      expect(yield* Projection.forget(bound).pipe(Effect.flip)).toBe(failure);
      expect(yield* store.load(bound)).toEqual(checkpoint);
      expect(yield* sql`SELECT id FROM forget_cleanup`).toEqual([]);
    }),
  );

  it.effect("transactionWithCleanup rolls back retirement and can retry after failure", () =>
    Effect.gen(function* () {
      const { params, rev, epoch, numberId, acceptedId } = binding("retire-with-cleanup");
      const store = yield* CheckpointStore;
      const target = yield* Numbers;
      const accepted = yield* Accepted;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE retire_cleanup (generation INTEGER NOT NULL)`;
      const failCleanup = yield* Ref.make(true);

      const failure = new ProjectionFault({
        phase: "checkpoint",
        reason: "storage-failure",
        message: "retire cleanup failed",
      });

      const commit = Commit.transactionWithCleanup((key) =>
        Effect.gen(function* () {
          expect(yield* store.load(key).pipe(Effect.flip, Effect.orDie)).toMatchObject(retired);
          expect(yield* target.head(rev).pipe(Effect.flip, Effect.orDie)).toMatchObject(missing);
          const client = yield* SqlClient.SqlClient;
          yield* client`INSERT INTO retire_cleanup (generation) VALUES (${key.generation})`.pipe(
            Effect.orDie,
          );

          if (yield* Ref.get(failCleanup)) return yield* failure;

          return yield* Effect.void;
        }),
      );

      const spec = numbers(commit, 1);
      const bound = spec.at(params);
      yield* seed(params);
      yield* Projection.run(bound);
      const checkpoint = yield* store.load(bound);
      expect(Option.isSome(checkpoint.record)).toBe(true);
      const acceptedBefore = (yield* accepted.collect(epoch)).items;
      expect(acceptedBefore).toHaveLength(1);

      const retire = Projection.retire(spec, params, { generation: 1 });
      expect(yield* retire.pipe(Effect.flip)).toBe(failure);
      expect(yield* store.load(bound)).toEqual(checkpoint);
      expect((yield* target.collect(rev)).items).toEqual([2]);
      expect((yield* accepted.collect(epoch)).items).toEqual(acceptedBefore);
      expect(yield* sql`SELECT generation FROM retire_cleanup`).toEqual([]);

      yield* Ref.set(failCleanup, false);
      expect(yield* retire).toEqual({
        record: "retired",
        streams: [
          { output: "@streamsy/accepted", id: acceptedId, result: "removed" },
          { output: "numbers", id: numberId, result: "removed" },
        ],
      });
      expect(yield* store.load(bound).pipe(Effect.flip)).toMatchObject(retired);
      expect(yield* target.head(rev).pipe(Effect.flip)).toMatchObject(missing);
      expect(yield* accepted.head(epoch).pipe(Effect.flip)).toMatchObject(missing);
      expect(yield* sql`SELECT generation FROM retire_cleanup`).toEqual([{ generation: 1 }]);

      expect((yield* retire).record).toBe("already-retired");
      expect(yield* sql`SELECT generation FROM retire_cleanup`).toEqual([
        { generation: 1 },
        { generation: 1 },
      ]);
    }),
  );

  for (const commit of [Commit.transaction, Commit.producer]) {
    it.effect(
      `${commit.name}: retire isolates generations, fences load and CAS, and survives forget`,
      () =>
        Effect.gen(function* () {
          const { params, rev, epoch, numberId, acceptedId } = binding(`isolation-${commit.name}`);
          yield* seed(params);
          const old = numbers(commit, 1).at(params);
          const serving = numbers(commit, 2);
          const current = serving.at(params);
          const store = yield* CheckpointStore;
          const records = yield* OwnerRecords;
          const target = yield* Numbers;
          const accepted = yield* Accepted;
          const currentNumber = Numbers.at({ ...params, rev: 2 }).id;
          const currentAccepted = Accepted.at({ ...params, epoch: 2 }).id;
          yield* Projection.run(old);
          yield* Projection.run(current);
          const live = yield* store.load(old);

          const readCurrent = Effect.all([
            records.read(recordKey(current)),
            target.wire.read(currentNumber, { offset: ZERO_OFFSET }),
            accepted.wire.read(currentAccepted, { offset: ZERO_OFFSET }),
          ]);

          const before = yield* readCurrent;
          const oldSlot = Option.getOrThrow(yield* records.read(recordKey(old)));

          expect(Schema.decodeOption(previousEnvelope)(oldSlot)._tag).toBe("Some");
          expect(yield* Projection.retire(serving, params, { generation: 1 })).toEqual({
            record: "retired",
            streams: [
              { output: "@streamsy/accepted", id: acceptedId, result: "removed" },
              { output: "numbers", id: numberId, result: "removed" },
            ],
          });
          expect(yield* readCurrent).toEqual(before);
          expect(yield* Projection.run(old).pipe(Effect.flip)).toMatchObject(retired);
          expect(yield* target.head(rev).pipe(Effect.flip)).toMatchObject(missing);
          expect(yield* accepted.head(epoch).pipe(Effect.flip)).toMatchObject(missing);
          expect(
            yield* store
              .compareAndSet(old, live.token, Option.getOrThrow(live.record))
              .pipe(Effect.flip),
          ).toMatchObject(retired);
          const tombstone = Option.getOrThrow(yield* records.read(recordKey(old)));

          expect(
            yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(tombstone),
          ).toEqual({
            version: Number(live.token) + 1,
            retired: {},
          });
          expect(Schema.decodeOption(previousEnvelope)(tombstone)).toEqual(Option.none());
          yield* Projection.forget(old);
          expect(yield* records.read(recordKey(old))).toEqual(Option.some(tombstone));
          expect(yield* Projection.retire(serving, params, { generation: 1 })).toEqual({
            record: "already-retired",
            streams: [
              { output: "@streamsy/accepted", id: acceptedId, result: "absent" },
              { output: "numbers", id: numberId, result: "absent" },
            ],
          });

          yield* Projection.forget(current);
          expect(yield* records.read(recordKey(current))).toEqual(Option.none());
        }),
    );

    for (const firstUnit of [false, true]) {
      it.effect(
        `${commit.name}: retire fences a loaded ${firstUnit ? "first" : "later"} unit`,
        () =>
          Effect.gen(function* () {
            const { params, rev, epoch } = binding(`loaded-${commit.name}-${firstUnit}`);
            yield* seed(params);
            const spec = numbers(commit, 1);
            const bound = spec.at(params);
            const input = yield* Facts;
            const target = yield* Numbers;
            const accepted = yield* Accepted;
            const loaded = yield* Deferred.make<void>();
            const resume = yield* Deferred.make<void>();

            if (!firstUnit) {
              yield* Projection.run(bound);
              yield* input.append(params, [{ id: "b", n: 3 }]);
            }

            const run = yield* Projection.run(bound).pipe(
              Effect.provideService(Facts, {
                ...input,
                wire: {
                  ...input.wire,
                  read: (id, options) =>
                    Deferred.succeed(loaded, undefined).pipe(
                      Effect.andThen(Deferred.await(resume)),
                      Effect.andThen(input.wire.read(id, options)),
                    ),
                },
              }),
              Effect.forkChild,
            );

            yield* Deferred.await(loaded);
            yield* Projection.retire(spec, params, { generation: 1 });
            yield* Deferred.succeed(resume, undefined);
            expect(yield* Fiber.join(run).pipe(Effect.flip)).toMatchObject(retired);
            expect(yield* target.head(rev).pipe(Effect.flip)).toMatchObject(missing);
            expect(yield* accepted.head(epoch).pipe(Effect.flip)).toMatchObject(missing);
          }),
      );
    }

    it.effect(`${commit.name}: cleanup failure can be retried`, () =>
      Effect.gen(function* () {
        const { params, rev, epoch } = binding(`cleanup-${commit.name}`);
        yield* seed(params);
        const spec = numbers(commit, 1);
        const bound = spec.at(params);
        const target = yield* Numbers;
        const accepted = yield* Accepted;
        const store = yield* CheckpointStore;
        yield* Projection.run(bound);
        const original = yield* store.load(bound);

        expect(
          yield* Projection.retire(spec, params, { generation: 1 }).pipe(
            Effect.provideService(Accepted, {
              ...accepted,
              wire: {
                ...accepted.wire,
                remove: (id) => new StreamBusy({ id }),
              },
            }),
            Effect.flip,
          ),
        ).toMatchObject({ phase: "checkpoint", reason: "output-busy", retryable: true });
        expect((yield* accepted.collect(epoch)).items).toHaveLength(1);

        if (commit === Commit.transaction) {
          expect(yield* store.load(bound)).toEqual(original);
          expect((yield* target.collect(rev)).items).toEqual([2]);
        } else {
          expect(yield* store.load(bound).pipe(Effect.flip)).toMatchObject(retired);
          expect(yield* target.head(rev).pipe(Effect.flip)).toMatchObject(missing);
        }

        const result = yield* Projection.retire(spec, params, { generation: 1 });
        expect(result.record).toBe(commit === Commit.transaction ? "retired" : "already-retired");
        expect(result.streams).toMatchObject([
          { output: "@streamsy/accepted", result: "removed" },
          { output: "numbers", result: commit === Commit.transaction ? "removed" : "absent" },
        ]);
      }),
    );
  }

  for (const streams of ["close", "remove"] as const) {
    it.effect(`producer: a unit pinned before retire cannot append after ${streams}`, () =>
      Effect.gen(function* () {
        const { params } = binding(`pinned-${streams}`);
        yield* seed(params);
        const spec = numbers(Commit.producer, 1);
        const bound = spec.at(params);
        const store = yield* CheckpointStore;
        const pinned = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();

        const run = yield* Projection.run(bound).pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: (key, token, next) =>
              store
                .compareAndSet(key, token, next)
                .pipe(
                  Effect.tap(() =>
                    next.pending === undefined
                      ? Effect.void
                      : Deferred.succeed(pinned, undefined).pipe(
                          Effect.andThen(Deferred.await(resume)),
                        ),
                  ),
                ),
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(pinned);
        expect(Option.getOrThrow((yield* store.load(bound)).record).pending).toBeDefined();
        yield* Projection.retire(spec, params, { generation: 1, streams });
        yield* Deferred.succeed(resume, undefined);
        expect(yield* Fiber.join(run).pipe(Effect.flip)).toMatchObject({
          phase: "pin",
          reason: "invalid-output",
        });
        expect(yield* store.load(bound).pipe(Effect.flip)).toMatchObject(retired);
      }),
    );
  }

  it.effect(
    "producer: a create already past the reload can leave an empty stream for a second retire",
    () =>
      Effect.gen(function* () {
        const { params, rev, numberId } = binding("late-create");
        yield* seed(params);
        const spec = numbers(Commit.producer, 1);
        const bound = spec.at(params);
        const target = yield* Numbers;
        const creating = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();

        const run = yield* Projection.run(bound).pipe(
          Effect.provideService(Numbers, {
            ...target,
            wire: {
              ...target.wire,
              create: (id, options) =>
                Deferred.succeed(creating, undefined).pipe(
                  Effect.andThen(Deferred.await(resume)),
                  Effect.andThen(target.wire.create(id, options)),
                ),
            },
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(creating);
        expect((yield* Projection.retire(spec, params, { generation: 1 })).streams).toContainEqual({
          output: "numbers",
          id: numberId,
          result: "absent",
        });
        yield* Deferred.succeed(resume, undefined);
        expect(yield* Fiber.join(run).pipe(Effect.flip)).toMatchObject(retired);
        expect((yield* target.head(rev)).nextOffset).toBe(ZERO_OFFSET);
        expect(yield* Projection.retire(spec, params, { generation: 1 })).toMatchObject({
          record: "already-retired",
          streams: expect.arrayContaining([{ output: "numbers", id: numberId, result: "removed" }]),
        });
      }),
  );

  it.effect(
    "close keeps data, skips shared and unwrapped streams, and can be followed by remove",
    () =>
      Effect.gen(function* () {
        const { params, rev, numberId, acceptedId } = binding("mixed");
        yield* seed(params);

        const spec = Projection.emit({
          ...declaration,
          id: "mixed-retirement",
          outputs: {
            numbers: output,
            plain: Numbers.at({ workspace: "plain", rev: 1 }),
          },
          accepted: Projection.perGeneration(Accepted, "epoch", { shared: true }),
          commit: Commit.transaction,
          emit: ({ item }) => ({ numbers: [item.n] }),
        });

        const wrapped = yield* Numbers;
        const wake = yield* Accepted;
        yield* wrapped.create({ workspace: "plain", rev: 1 });
        yield* wrapped.append({ workspace: "plain", rev: 1 }, [9]);
        yield* Projection.run(spec.at(params));
        const acceptedBefore = yield* wake.wire.read(acceptedId, { offset: ZERO_OFFSET });

        for (const record of ["retired", "already-retired"]) {
          expect(
            yield* Projection.retire(spec, params, { generation: 1, streams: "close" }),
          ).toEqual({
            record,
            streams: [
              { output: "@streamsy/accepted", id: acceptedId, result: "shared" },
              { output: "numbers", id: numberId, result: "closed" },
            ],
          });
        }

        expect((yield* wrapped.collect(rev)).items).toEqual([2]);
        expect((yield* wrapped.head(rev)).closed).toBe(true);
        expect(yield* Projection.retire(spec, params, { generation: 1 })).toMatchObject({
          record: "already-retired",
          streams: expect.arrayContaining([
            { output: "numbers", result: "removed", id: expect.any(String) },
          ]),
        });
        expect(yield* wrapped.head(rev).pipe(Effect.flip)).toMatchObject(missing);
        expect((yield* wrapped.collect({ workspace: "plain", rev: 1 })).items).toEqual([9]);
        expect(yield* wake.wire.read(acceptedId, { offset: ZERO_OFFSET })).toEqual(acceptedBefore);
      }),
  );

  it.effect("soft-deleted outputs report removed once and absent in either mode thereafter", () =>
    Effect.gen(function* () {
      const { params, rev, numberId } = binding("soft-delete");
      yield* seed(params);
      const spec = numbers(Commit.transaction, 1);
      const target = yield* Numbers;
      yield* Projection.run(spec.at(params));
      yield* target.wire.fork(Numbers.at({ ...params, rev: 3 }).id, numberId);
      expect((yield* Projection.retire(spec, params, { generation: 1 })).streams).toContainEqual({
        output: "numbers",
        id: numberId,
        result: "removed",
      });
      expect(yield* target.head(rev).pipe(Effect.flip)).toMatchObject({ _tag: "StreamGone" });

      for (const streams of ["close", "remove"] as const)
        expect(
          (yield* Projection.retire(spec, params, { generation: 1, streams })).streams,
        ).toContainEqual({ output: "numbers", id: numberId, result: "absent" });
      expect((yield* target.collect({ ...params, rev: 3 })).items).toEqual([2]);
    }),
  );

  it.effect(
    "transaction retirement rejects a foreign output and producer retirement can remove it",
    () =>
      Effect.gen(function* () {
        const { params, rev, epoch } = binding("foreign-owner");
        const transaction = numbers(Commit.transaction, 1);
        const producer = numbers(Commit.producer, 1);
        const bound = transaction.at(params);
        const local = yield* Numbers;
        const store = yield* CheckpointStore;

        const foreign = Context.get(
          yield* Layer.build(withFamilies(layerSqlite(), Accepted)),
          Accepted,
        );

        yield* seed(params);
        yield* Projection.run(bound);
        const live = yield* store.load(bound);
        yield* foreign.create(epoch);

        expect(
          yield* Projection.retire(transaction, params, { generation: 1 }).pipe(
            Effect.provideService(Accepted, foreign),
            Effect.flip,
          ),
        ).toMatchObject(unsupported);
        expect(yield* store.load(bound)).toEqual(live);
        expect((yield* local.collect(rev)).items).toEqual([2]);
        expect((yield* foreign.head(epoch)).closed).toBe(false);

        expect(
          yield* Projection.retire(producer, params, { generation: 1 }).pipe(
            Effect.provideService(Accepted, foreign),
          ),
        ).toMatchObject({
          record: "retired",
          streams: expect.arrayContaining([
            { output: "@streamsy/accepted", result: "removed", id: expect.any(String) },
          ]),
        });
        expect(yield* local.head(rev).pipe(Effect.flip)).toMatchObject(missing);
        expect(yield* foreign.head(epoch).pipe(Effect.flip)).toMatchObject(missing);
        expect(yield* store.load(bound).pipe(Effect.flip)).toMatchObject(retired);
      }),
  );

  it.effect(
    "an absent checkpoint can retire existing outputs and createOutputs remains a stream API",
    () =>
      Effect.gen(function* () {
        const { params, rev } = binding("explicit-create");
        const spec = numbers(Commit.producer, 1);
        const bound = spec.at(params);
        const target = yield* Numbers;
        const store = yield* CheckpointStore;
        yield* Projection.createOutputs(bound);
        expect((yield* store.load(bound)).record).toEqual(Option.none());

        expect(yield* Projection.retire(spec, params, { generation: 1 })).toMatchObject({
          record: "retired",
          streams: expect.arrayContaining([
            { output: "numbers", result: "removed", id: expect.any(String) },
          ]),
        });
        yield* Projection.createOutputs(bound);
        expect((yield* target.head(rev)).nextOffset).toBe(ZERO_OFFSET);
        expect(yield* Projection.run(bound).pipe(Effect.flip)).toMatchObject(retired);
      }),
  );

  it.effect(
    "checkpoint and atLeastOnce retire legacy bindings and optional hooks fail cleanly",
    () =>
      Effect.gen(function* () {
        const { params } = binding("legacy");
        yield* seed(params);
        const store = yield* CheckpointStore;

        const fold = Projection.fold({
          ...declaration,
          id: "retire-fold",
          state: { schema: Schema.Finite, initial: 0 },
          fold: (sum, { item }) => sum + item.n,
        });

        const sideEffect = Projection.process({
          ...declaration,
          id: "retire-side-effect",
          commit: Commit.atLeastOnce,
          process: () => Effect.void,
        });

        const custom = Projection.process({
          ...declaration,
          id: "retire-custom",
          commit: Commit.make({
            name: "custom",
            guarantee: "at-least-once",
            load: Commit.atLeastOnce.load,
            apply: Commit.atLeastOnce.apply,
          }),
          process: () => Effect.void,
        });

        const oldStore = {
          load: store.load,
          compareAndSet: store.compareAndSet,
          remove: store.remove,
        };

        expect(
          yield* Projection.retire(custom, params, { generation: 1 }).pipe(Effect.flip),
        ).toMatchObject(unsupported);
        expect(
          yield* Projection.retire(fold, params, { generation: 1 }).pipe(
            Effect.provideService(CheckpointStore, oldStore),
            Effect.flip,
          ),
        ).toMatchObject(unsupported);

        yield* Projection.run(fold.at(params));
        yield* Projection.run(sideEffect.at(params));

        for (const spec of [fold, sideEffect]) {
          expect(yield* Projection.retire(spec, params, { generation: 1 })).toEqual({
            record: "retired",
            streams: [],
          });
        }

        expect(yield* Projection.run(fold.at(params)).pipe(Effect.flip)).toMatchObject(retired);
        expect(yield* Projection.run(sideEffect.at(params)).pipe(Effect.flip)).toMatchObject(
          retired,
        );
        expect((yield* (yield* Facts).collect(params)).items).toEqual([{ id: "a", n: 2 }]);
      }),
  );

  it.effect(
    "StateTable.clear drops only one index, works after retire, and allows replay from a live stream",
    () =>
      Effect.gen(function* () {
        const { params, rev } = binding("state-table");
        yield* seed(params);
        yield* StateTable.migrate(yield* SqlClient.SqlClient);
        yield* Projection.run(sums(1).at(params));
        yield* Projection.run(sums(2).at(params));
        const old = table.at(rev);
        const current = table.at({ ...params, rev: 2 });
        const store = yield* CheckpointStore;
        yield* Projection.run(old);
        yield* Projection.run(current);
        const checkpoint = yield* store.load(old);
        const rows = yield* old.list("sum");
        expect(rows.rows).toEqual([{ key: "a", value: { id: "a", count: 2 } }]);

        yield* StateTable.clear(old);
        expect(yield* old.list("sum")).toMatchObject({ offset: ZERO_OFFSET, rows: [] });
        expect(yield* store.load(old)).toEqual(checkpoint);
        expect(yield* current.list("sum")).toEqual(rows);
        expect((yield* (yield* Rows).collect(rev)).items).toHaveLength(1);
        expect((yield* Projection.run(old)).units).toBe(0);

        yield* StateTable.rebuild(old);
        expect(yield* old.list("sum")).toEqual(rows);
        expect(yield* Projection.retire(table, rev, { generation: table.generation })).toEqual({
          record: "retired",
          streams: [],
        });
        expect((yield* (yield* Rows).collect(rev)).items).toHaveLength(1);
        yield* Projection.retire(sums(2), params, { generation: 1 });
        yield* StateTable.clear(old);
        yield* StateTable.clear(old);
        expect(yield* old.list("sum")).toMatchObject({ offset: ZERO_OFFSET, rows: [] });
        expect(yield* current.list("sum")).toEqual(rows);
      }),
  );
});

layer(CheckpointStore.layerMemory)((it) => {
  it.effect("memory checkpoints can forget live records and preserve tombstones", () =>
    Effect.gen(function* () {
      const store = yield* CheckpointStore;

      const spec = Projection.process({
        id: "memory-retire",
        params: {},
        inputs: {},
        commit: Commit.atLeastOnce,
        process: () => Effect.void,
      });

      const bound = spec.at({});
      const record = { identity: { inputs: {} }, inputs: {}, adapters: {} };
      yield* store.compareAndSet(bound, "0", record);
      yield* Projection.forget(bound);
      expect((yield* store.load(bound)).token).toBe("0");
      yield* Projection.retire(spec, {}, { generation: 1 });
      yield* Projection.forget(bound);
      expect(yield* store.compareAndSet(bound, "0", record).pipe(Effect.flip)).toMatchObject(
        retired,
      );
    }),
  );
});

layer(Projection.Registry.layer.pipe(Layer.provideMerge(host)))((it) => {
  it.effect("a retired binding stops on its first registry pass", () =>
    Effect.gen(function* () {
      const { params, rev } = binding("registry");
      yield* seed(params);
      const spec = numbers(Commit.transaction, 1);
      yield* Projection.retire(spec, params, { generation: 1 });
      const wakeup = yield* ProjectionWakeup.attach(spec);
      const registry = yield* Projection.Registry;
      yield* registry.schedule(spec.at(params));
      yield* wakeup.run;
      yield* wakeup.run;
      expect(yield* registry.failed(spec)).toEqual([
        expect.objectContaining({
          attempts: 1,
          stopped: true,
          failure: Option.some(
            expect.objectContaining({
              reason: Option.some("retired-generation"),
            }),
          ),
        }),
      ]);
      expect(yield* (yield* Numbers).head(rev).pipe(Effect.flip)).toMatchObject(missing);
    }),
  );
});
