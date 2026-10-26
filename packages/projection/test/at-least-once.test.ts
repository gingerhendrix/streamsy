import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Layer, Cause, Effect, Exit, Option, Schema } from "effect";
import { Streamsy, MemoryStorage } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection, ProjectionFault } from "@streamsy/projection";

class Input extends Streamsy.json<Input>()("atLeastOnce/Input", "at-least-once/input", {
  params: {},
  schema: Schema.Finite,
}) {}

const host = Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Input));

const seed = Effect.gen(function* () {
  const input = yield* Input;
  yield* input.create({});
  yield* input.append({}, [1]);
});

const fault = new ProjectionFault({
  phase: "checkpoint",
  reason: "storage-failure",
  message: "settle failed",
});

for (const failHandler of [true, false]) {
  layer(Layer.merge(host, CheckpointStore.layerMemory))((it) => {
    it.effect(
      `atLeastOnce keeps the range after ${failHandler ? "handler" : "settle"} failure`,
      () =>
        Effect.gen(function* () {
          yield* seed;
          const keys: string[] = [];
          const batches: number[][] = [];
          let failed = false;
          const compensations: string[] = [];

          const base = Projection.process({
            commit: Commit.atLeastOnce,
            id: "effect",
            params: {},
            inputs: { input: Input },
            process: (batch, unit) =>
              Effect.gen(function* () {
                keys.push(unit.key);
                batches.push([...batch.input.items]);

                if (failHandler && !failed) {
                  failed = true;

                  return yield* Effect.fail("handler-error");
                }

                return yield* Effect.void;
              }),
          }).at({});

          const projection = {
            ...base,
            commit: Commit.atLeastOnce.pipe(
              Commit.withCompensation((_result, _cause, unit) =>
                Effect.sync(() => {
                  compensations.push(unit.key);
                }),
              ),
            ),
          };

          const store = yield* CheckpointStore;

          const first = yield* Projection.pass(projection).pipe(
            Effect.provideService(CheckpointStore, {
              ...store,
              compareAndSet: (key, token, record) =>
                !failHandler && record.pending === undefined
                  ? Effect.fail(fault)
                  : store.compareAndSet(key, token, record),
            }),
            Effect.result,
          );

          expect(first._tag).toBe("Failure");

          if (first._tag === "Failure")
            expect(first.failure).toEqual(failHandler ? "handler-error" : fault);
          const pinned = Option.getOrThrow((yield* store.load(projection)).record);
          expect(pinned.pending).toBeDefined();
          expect(pinned.pending?.seqs).toBeUndefined();
          yield* (yield* Input).append({}, [2]);
          yield* Projection.pass(projection);
          expect(keys[1]).toBe(keys[0]);
          expect(batches).toEqual([[1], [1]]);
          expect(compensations).toEqual(failHandler ? [] : keys.slice(0, 1));
          expect(Option.getOrThrow((yield* store.load(projection)).record).pending).toBeUndefined();
          yield* Projection.run(projection);
          expect(batches).toEqual([[1], [1], [2]]);
        }),
    );
  });
}

layer(Layer.merge(host, CheckpointStore.layerMemory))((it) => {
  it.effect("compensation observes successful void and preserves a store defect", () =>
    Effect.gen(function* () {
      yield* seed;
      const defect = new Error("store defect");
      const observed: unknown[] = [];

      const base = Projection.process({
        commit: Commit.atLeastOnce,
        id: "defect",
        params: {},
        inputs: { input: Input },
        process: () => Effect.void,
      }).at({});

      const projection = {
        ...base,
        commit: Commit.withCompensation(Commit.atLeastOnce, (result, cause) =>
          Effect.sync(() => {
            observed.push(result, Cause.squash(cause));
          }),
        ),
      };

      const store = yield* CheckpointStore;

      const exit = yield* Projection.run(projection).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: (key, token, record) =>
            record.pending === undefined
              ? Effect.die(defect)
              : store.compareAndSet(key, token, record),
        }),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);

      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(defect);
      expect(observed).toEqual([undefined, defect]);
    }),
  );
});

layer(Layer.merge(host, CheckpointStore.layerMemory))((it) => {
  it.effect("a failed pin never runs the handler and a custom Commit cannot inherit pinning", () =>
    Effect.gen(function* () {
      yield* seed;
      let calls = 0;

      const base = Projection.process({
        commit: Commit.atLeastOnce,
        id: "pin-first",
        params: {},
        inputs: { input: Input },
        process: () => {
          calls += 1;

          return Effect.void;
        },
      }).at({});

      const projection = { ...base, commit: Commit.atLeastOnce };
      const store = yield* CheckpointStore;

      const blocked = yield* Projection.run(projection).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(fault),
        }),
        Effect.result,
      );

      expect(blocked._tag).toBe("Failure");
      expect(calls).toBe(0);
      yield* Projection.run({ ...projection, process: () => Effect.fail("keep pin") }).pipe(
        Effect.result,
      );

      const custom = Commit.make({
        name: "custom",
        guarantee: "at-least-once",
        load: Commit.atLeastOnce.load,
        apply: Commit.atLeastOnce.apply,
      });

      const rejected = yield* Projection.run({ ...projection, commit: custom }).pipe(Effect.result);
      expect(rejected).toMatchObject({
        _tag: "Failure",
        failure: { phase: "load", reason: "invalid-record" },
      });
      expect(calls).toBe(0);
    }),
  );
});

layer(Layer.merge(host, CheckpointStore.layerMemory))((it) => {
  it.effect(
    "a removal after an at-least-once pin keeps the pin and fails range-unreproducible",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const seen: { items: readonly number[]; gone: boolean }[] = [];

        const bound = Projection.process({
          id: "gone-pending",
          params: {},
          inputs: { input: Input },
          commit: Commit.atLeastOnce,
          process: (batch) =>
            Effect.sync(() => {
              seen.push({ items: batch.input.items, gone: batch.input.gone === true });
            }),
        }).at({});

        const store = yield* CheckpointStore;
        expect(
          (yield* Projection.run(bound).pipe(
            Effect.provideService(CheckpointStore, {
              ...store,
              compareAndSet: (key, token, record) =>
                record.pending === undefined
                  ? Effect.fail(fault)
                  : store.compareAndSet(key, token, record),
            }),
            Effect.result,
          ))._tag,
        ).toBe("Failure");
        yield* (yield* Input).remove({});
        const result = yield* Projection.run(bound).pipe(Effect.result);
        expect(result._tag === "Failure" && result.failure.reason).toBe("range-unreproducible");
        expect(seen).toEqual([{ items: [1], gone: false }]);
        expect(Option.isSome((yield* store.load(bound)).record)).toBe(true);
      }),
  );
});
