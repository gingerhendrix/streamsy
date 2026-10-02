import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Schema, type Scope } from "effect";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { layerMemory } from "./support/memory.ts";

type Services =
  | CheckpointStore
  | import("@streamsy/core").Streamsy.Wildcard
  | StreamsReader
  | StreamsWriter
  | Scope.Scope;
const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(
    body.pipe(Effect.scoped, Effect.provide(FamilyFixture.withFamilies(layerMemory()))),
  );

test("two calls on one key run in sequence without a token conflict", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("serialized-one", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "serialized-one",
        params: {},
        inputs: { input },
        process: () =>
          Effect.gen(function* () {
            calls += 1;
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
          }),
      }).at({});
      const first = yield* Projection.run(projection).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      const second = yield* Projection.run(projection).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      expect(calls).toBe(1);
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(first)).status).toBe("caught-up");
      expect((yield* Fiber.join(second)).status).toBe("caught-up");
      expect(calls).toBe(1);
    }),
  ));

test("different keys can run together", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("serialized-two", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const both = yield* Deferred.make<void>();
      let active = 0;
      let maximum = 0;
      const make = (name: string) => {
        const output = FamilyFixture.json(`serialized-two-output-${name}`, {
          schema: Schema.Finite,
        });
        return Projection.emit({
          commit: Commit.producer,
          id: "serialized-two",
          params: { name: Schema.String },
          inputs: { input },
          outputs: { output: output },
          emit: () =>
            Effect.acquireUseRelease(
              Effect.gen(function* () {
                yield* Effect.sync(() => {
                  active += 1;
                  maximum = Math.max(maximum, active);
                });
                if (active === 2) yield* Deferred.succeed(both, undefined);
              }),
              () => Deferred.await(both).pipe(Effect.as({ output: [1] })),
              () => Effect.sync(() => (active -= 1)),
            ),
        }).at({ name });
      };
      yield* Effect.all([Projection.run(make("a")), Projection.run(make("b"))], {
        concurrency: "unbounded",
      });
      expect(maximum).toBe(2);
    }),
  ));

test("a failing handler releases the permit", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("serialized-failure", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      let fail = true;
      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "serialized-failure",
        params: {},
        inputs: { input },
        process: () =>
          Effect.suspend(() => {
            if (fail) {
              fail = false;
              return Effect.fail("handler-failure" as const);
            }
            return Effect.void;
          }),
      }).at({});
      expect((yield* Projection.run(projection).pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* Projection.run(projection)).status).toBe("caught-up");
    }),
  ));

test("follow and run share the permit and release it after interruption", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("follow-permit", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1], { close: true });
      const entered = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "follow-permit",
        params: {},
        inputs: { input },
        process: () =>
          Effect.gen(function* () {
            calls += 1;
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              return yield* Effect.never;
            }
          }),
      }).at({});
      const follower = yield* Projection.follow(projection);
      yield* Deferred.await(entered);
      const request = yield* Projection.run(projection).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      expect(calls).toBe(1);
      yield* Fiber.interrupt(follower);
      expect((yield* Fiber.join(request)).status).toBe("source-closed");
      expect(calls).toBe(2);
    }),
  ));
