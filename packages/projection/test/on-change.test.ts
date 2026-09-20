import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Option, Schema, Stream, type Scope } from "effect";
import {
  Storage,
  StreamRef,
  StreamRoute,
  Streams,
  type StreamsReader,
  type StreamsWriter,
} from "@streamsy/core";
import { Checkpoints, Projection } from "@streamsy/projection";
import { layerMemory } from "@streamsy/projection/memory";

type Services = Checkpoints | Storage | StreamsReader | StreamsWriter | Scope.Scope;
const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(body.pipe(Effect.scoped, Effect.provide(layerMemory())));
const readAll = <A>(ref: StreamRef.StreamRef<A>) =>
  Streams.read(ref).pipe(Streams.items, Stream.runCollect);

test("an append runs a fused projection and commits its write with the checkpoint", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-fused-input", { schema: Schema.Finite });
      const output = StreamRef.json("change-fused-output", { schema: Schema.Finite });
      yield* Streams.create(input);
      yield* Streams.create(output);
      const handled = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.make({
        id: "change-fused",
        input,
        process: (batch) =>
          Streams.append(output, batch.input.items).pipe(
            Effect.tap(() => Effect.sync(() => (calls += 1))),
            Effect.tap(() => Deferred.succeed(handled, undefined)),
          ),
      });
      const fiber = yield* Projection.onChange(projection);
      yield* Streams.append(input, [1]);
      yield* Deferred.await(handled);
      yield* Projection.serialized(projection);
      expect(calls).toBe(1);
      expect(yield* readAll(output)).toEqual([1]);
      expect(
        Option.isSome((yield* (yield* Checkpoints).load(Projection.key(projection))).record),
      ).toBe(true);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("changes during an in-flight run coalesce to one follow-up run", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-coalesce-input", { schema: Schema.Finite });
      const output = StreamRef.json("change-coalesce-output", { schema: Schema.Finite });
      yield* Streams.create(input);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const second = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.stream({
        id: "change-coalesce",
        input,
        output,
        process: (batch) =>
          Effect.gen(function* () {
            calls += 1;
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            } else yield* Deferred.succeed(second, undefined);
            return batch.input.items;
          }),
      });
      const fiber = yield* Projection.onChange(projection);
      yield* Streams.append(input, [1]);
      yield* Deferred.await(entered);
      yield* Streams.append(input, [2]);
      yield* Streams.append(input, [3]);
      yield* Streams.append(input, [4]);
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(second);
      yield* Projection.serialized(projection);
      expect(calls).toBe(2);
      expect(yield* readAll(output)).toEqual([1, 2, 3, 4]);
      const tail = yield* Streams.head(input);
      const loaded = yield* (yield* Checkpoints).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("onChange and a serialized request share the same key lock", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-request-input", { schema: Schema.Finite });
      const output = StreamRef.json("change-request-output", { schema: Schema.Finite });
      yield* Streams.create(input);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const projection = Projection.stream({
        id: "change-request",
        input,
        output,
        process: (batch) =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as(batch.input.items),
          ),
      });
      const watcher = yield* Projection.onChange(projection);
      yield* Streams.append(input, [1]);
      yield* Deferred.await(entered);
      const request = yield* Projection.serialized(projection).pipe(Effect.forkScoped);
      expect(request.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(request)).status).toBe("caught-up");
      expect(yield* readAll(output)).toEqual([1]);
      yield* Fiber.interrupt(watcher);
    }),
  ));

test("a family watches its fixed member set independently", () =>
  run(
    Effect.gen(function* () {
      const route = StreamRoute.json("change-family/:workspaceId", {
        params: { workspaceId: Schema.String },
        schema: Schema.Finite,
      });
      const seen: Array<string> = [];
      const a = yield* Deferred.make<void>();
      const b = yield* Deferred.make<void>();
      const family = Projection.family({
        id: "change-family",
        params: { workspaceId: Schema.String },
        inputs: { facts: route },
        process: (_batch, unit) =>
          Effect.gen(function* () {
            const workspaceId = unit.params.workspaceId ?? "";
            seen.push(workspaceId);
            yield* Deferred.succeed(workspaceId === "a" ? a : b, undefined);
          }),
      });
      const aRef = route.ref({ workspaceId: "a" });
      const bRef = route.ref({ workspaceId: "b" });
      yield* Streams.create(aRef);
      yield* Streams.create(bRef);
      const fiber = yield* Projection.onChange(family, [
        { workspaceId: "a" },
        { workspaceId: "b" },
      ]);
      yield* Streams.append(aRef, [1]);
      yield* Deferred.await(a);
      expect(seen).toEqual(["a"]);
      yield* Streams.append(bRef, [2]);
      yield* Deferred.await(b);
      expect(seen).toEqual(["a", "b"]);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("closing the caller scope ends the watcher fiber", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-scope", { schema: Schema.Finite });
      yield* Streams.create(input);
      const projection = Projection.make({
        id: "change-scope",
        input,
        process: () => Effect.void,
      });
      const fiber = yield* Effect.scoped(Projection.onChange(projection));
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
    }),
  ));
