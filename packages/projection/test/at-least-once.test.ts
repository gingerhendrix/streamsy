import { expect, test } from "bun:test";
import { Cause, Effect, Exit, Option, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Protocol, Streams } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection, ProjectionFault } from "@streamsy/projection";

class Input extends Streamsy.json<Input>()("atLeastOnce/Input", "at-least-once/input", {
  params: {},
  schema: Schema.Number,
}) {}
const host = Protocol.bind(Streams.layerMemory(), Streamsy.group(Input));
const run = <A, E>(effect: Effect.Effect<A, E, Input | CheckpointStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(host), Effect.provide(CheckpointStore.layerMemory)));
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
  test(`atLeastOnce keeps the range after ${failHandler ? "handler" : "settle"} failure`, () =>
    run(
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
    ));
}

test("compensation observes successful void and preserves a store defect", () =>
  run(
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
  ));

test("a failed pin never runs the handler and a custom Commit cannot inherit pinning", () =>
  run(
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
  ));
