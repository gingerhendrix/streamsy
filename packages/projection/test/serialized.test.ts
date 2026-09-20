import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Schema, type Scope } from "effect";
import { StreamRef, Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core";
import { Checkpoints, Projection } from "@streamsy/projection";
import { layerMemory } from "@streamsy/projection/memory";

type Services = Checkpoints | StreamsReader | StreamsWriter | Scope.Scope;
const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(body.pipe(Effect.scoped, Effect.provide(layerMemory())));

test("two calls on one key run in sequence without a token conflict", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("serialized-one", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.make({
        id: "serialized-one",
        input,
        process: () =>
          Effect.gen(function* () {
            calls += 1;
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
          }),
      });
      const first = yield* Projection.serialized(projection).pipe(Effect.forkScoped);
      yield* Deferred.await(entered);
      const second = yield* Projection.serialized(projection).pipe(Effect.forkScoped);
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
      const input = StreamRef.json("serialized-two", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const both = yield* Deferred.make<void>();
      let active = 0;
      let maximum = 0;
      const make = (name: string) => {
        const output = StreamRef.json(`serialized-two-output-${name}`, { schema: Schema.Finite });
        return Projection.stream({
          id: "serialized-two",
          params: { name },
          input,
          output,
          process: () =>
            Effect.acquireUseRelease(
              Effect.gen(function* () {
                yield* Effect.sync(() => {
                  active += 1;
                  maximum = Math.max(maximum, active);
                });
                if (active === 2) yield* Deferred.succeed(both, undefined);
              }),
              () => Deferred.await(both).pipe(Effect.as([1])),
              () => Effect.sync(() => (active -= 1)),
            ),
        });
      };
      yield* Effect.all([Projection.serialized(make("a")), Projection.serialized(make("b"))], {
        concurrency: "unbounded",
      });
      expect(maximum).toBe(2);
    }),
  ));

test("a failing handler releases the permit", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("serialized-failure", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      let fail = true;
      const projection = Projection.make({
        id: "serialized-failure",
        input,
        process: () =>
          Effect.suspend(() => {
            if (fail) {
              fail = false;
              return Effect.fail("handler-failure" as const);
            }
            return Effect.void;
          }),
      });
      expect((yield* Projection.serialized(projection).pipe(Effect.result))._tag).toBe("Failure");
      expect((yield* Projection.serialized(projection)).status).toBe("caught-up");
    }),
  ));
