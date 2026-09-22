import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Option, Schema, Stream, type Scope } from "effect";
import {
  Storage,
  StreamGone,
  StreamRef,
  StreamRoute,
  Streams,
  StreamsReader,
  StreamsWriter,
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
      const input = StreamRef.json("change-fused-input", {
        schema: Schema.Finite,
      });
      const output = StreamRef.json("change-fused-output", {
        schema: Schema.Finite,
      });
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
      const input = StreamRef.json("change-coalesce-input", {
        schema: Schema.Finite,
      });
      const output = StreamRef.json("change-coalesce-output", {
        schema: Schema.Finite,
      });
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

test("a limited wake drains the backlog before waiting for another change", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const input = StreamRef.json("change-limit-input", {
        schema: Schema.Finite,
      });
      yield* Streams.create(input);
      yield* Streams.append(input, [1, 2, 3]);
      const seen: Array<number> = [];
      const drained = yield* Deferred.make<void>();
      const projection = Projection.make({
        id: "change-limit",
        input,
        process: (batch) =>
          Effect.gen(function* () {
            seen.push(...batch.input.items);
            if (seen.length === 3) yield* Deferred.succeed(drained, undefined);
          }),
      });
      const fiber = yield* Projection.onChange(projection, { limit: 1 });
      yield* Deferred.await(drained);
      yield* Projection.serialized(projection);
      expect(seen).toEqual([1, 2, 3]);
      const tail = yield* Streams.head(input);
      const loaded = yield* (yield* Checkpoints).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(fiber);
    }).pipe(Effect.scoped, Effect.provide(layerMemory({ readLimit: 1 }))),
  ));

test("an invalid limit fails before a watcher fiber is created", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-invalid-limit", {
        schema: Schema.Finite,
      });
      const projection = Projection.make({
        id: "change-invalid-limit",
        input,
        process: () => Effect.void,
      });
      const result = yield* Projection.onChange(projection, { limit: 0 }).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure.phase).toBe("load");
        expect(result.failure.reason).toBe("invalid-options");
      }
    }),
  ));

test("an ownership probe reports a gone input as unavailable history", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-probe-failure", { schema: Schema.Finite });
      const projection = Projection.make({
        id: "change-probe-failure",
        input,
        process: () => Effect.void,
      });
      const reader = yield* StreamsReader;
      const fiber = yield* Projection.onChange(projection).pipe(
        Effect.provideService(StreamsReader, {
          ...reader,
          head: (id) => Effect.fail(new StreamGone({ id })),
        }),
      );
      const result = yield* Fiber.join(fiber).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure.phase).toBe("read");
        expect(result.failure.reason).toBe("history-unavailable");
        expect(result.failure.input).toBe("input");
      }
    }),
  ));

test("an empty family without Storage reports unsupported composition", async () => {
  const route = StreamRoute.json("change-empty-family/:workspaceId", {
    params: { workspaceId: Schema.String },
    schema: Schema.Finite,
  });
  const family = Projection.family({
    id: "change-empty-family",
    params: { workspaceId: Schema.String },
    inputs: { facts: route },
    process: () => Effect.void,
  });
  const unused = Effect.die("unused host service");
  const checkpoints = Checkpoints.of({
    load: () => unused,
    save: () => unused,
    remove: () => unused,
    withTransaction: (body) => body,
  });
  const reader = StreamsReader.of({
    head: () => unused,
    read: () => unused,
    readNext: () => unused,
  });
  const writer = StreamsWriter.of({
    create: () => unused,
    fork: () => unused,
    append: () => unused,
    remove: () => unused,
  });
  const result = await Effect.runPromise(
    Projection.onChange(family, []).pipe(
      Effect.result,
      Effect.scoped,
      Effect.provideService(Checkpoints, checkpoints),
      Effect.provideService(StreamsReader, reader),
      Effect.provideService(StreamsWriter, writer),
    ),
  );
  expect(result._tag).toBe("Failure");
  if (result._tag === "Failure") {
    expect(result.failure.reason).toBe("unsupported-composition");
    expect(result.failure.input).toBeUndefined();
  }
});

test("onChange and a serialized request share the same key lock", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-request-input", {
        schema: Schema.Finite,
      });
      const output = StreamRef.json("change-request-output", {
        schema: Schema.Finite,
      });
      yield* Streams.create(input);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.stream({
        id: "change-request",
        input,
        output,
        process: (batch) =>
          Effect.sync(() => (calls += 1)).pipe(
            Effect.andThen(Deferred.succeed(entered, undefined)),
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
      expect(calls).toBe(1);
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
      const fibers = yield* Projection.onChange(family, [
        { workspaceId: "a" },
        { workspaceId: "b" },
      ]);
      yield* Streams.append(aRef, [1]);
      yield* Deferred.await(a);
      expect(seen).toEqual(["a"]);
      yield* Streams.append(bRef, [2]);
      yield* Deferred.await(b);
      expect(seen).toEqual(["a", "b"]);
      yield* Effect.forEach(fibers, Fiber.interrupt);
    }),
  ));

test("closing the caller scope interrupts every family member fiber", () =>
  run(
    Effect.gen(function* () {
      const route = StreamRoute.json("change-family-scope/:workspaceId", {
        params: { workspaceId: Schema.String },
        schema: Schema.Finite,
      });
      const family = Projection.family({
        id: "change-family-scope",
        params: { workspaceId: Schema.String },
        inputs: { facts: route },
        process: () => Effect.void,
      });
      const fibers = yield* Effect.scoped(
        Projection.onChange(family, [{ workspaceId: "a" }, { workspaceId: "b" }]),
      );
      for (const fiber of fibers) expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
    }),
  ));

test("a closed and drained input ends its watcher", () =>
  run(
    Effect.gen(function* () {
      const input = StreamRef.json("change-closed", { schema: Schema.Finite });
      yield* Streams.create(input);
      const projection = Projection.make({
        id: "change-closed",
        input,
        process: () => Effect.void,
      });
      const fiber = yield* Projection.onChange(projection);
      yield* Streams.append(input, [1]);
      yield* Streams.append(input, [], { close: true });
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
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
      expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
    }),
  ));
