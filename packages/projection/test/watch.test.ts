import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { Streamsy } from "@streamsy/core";
import { expect, test } from "bun:test";
import { Deferred, Effect, Exit, Fiber, Option, Schema, Stream, type Scope } from "effect";
import { Storage, StreamGone, StreamAddress } from "@streamsy/core";
import { Streams, StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Output, Projection, type Transaction } from "@streamsy/projection";
import { layerMemory } from "@streamsy/projection/memory";

class WatchRoute2 extends Streamsy.json<WatchRoute2>()(
  "on-change.test/WatchRoute2",
  "change-stream-scope/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    schema: Schema.Finite,
  },
) {}
class WatchRoute1 extends Streamsy.json<WatchRoute1>()(
  "on-change.test/WatchRoute1",
  "change-stream/:workspaceId",
  {
    params: { workspaceId: Schema.String },
    schema: Schema.Finite,
  },
) {}

type Services =
  | import("@streamsy/core").Streamsy.ChangeFeed<
      WatchRoute1 | WatchRoute2 | import("@streamsy/core").Streamsy.Wildcard
    >
  | WatchRoute2
  | WatchRoute1
  | CheckpointStore
  | Transaction
  | Storage
  | import("@streamsy/core").Streamsy.Wildcard
  | StreamsReader
  | StreamsWriter
  | Scope.Scope;
const run = <A, E>(body: Effect.Effect<A, E, Services>) =>
  Effect.runPromise(
    body.pipe(
      Effect.scoped,
      Effect.provide(FamilyFixture.withFamilies(layerMemory(), WatchRoute1, WatchRoute2)),
    ),
  );
const readAll = <A>(address: StreamAddress.StreamAddress<A>) =>
  Streams.read(address).pipe(Streams.items, Stream.runCollect);

test("an append runs an atomic projection and commits its write with the checkpoint", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-atomic-input", {
        schema: Schema.Finite,
      });
      const output = FamilyFixture.json("change-atomic-output", {
        schema: Schema.Finite,
      });
      yield* Streams.create(input);
      yield* Streams.create(output);
      const handled = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "change-atomic",
        params: {},
        inputs: { input },
        process: (batch) =>
          Streams.append(output, batch.input.items).pipe(
            Effect.tap(() => Effect.sync(() => (calls += 1))),
            Effect.tap(() => Deferred.succeed(handled, undefined)),
          ),
      }).at({});
      const fiber = yield* Projection.watch(projection);
      yield* Streams.append(input, [1]);
      yield* Deferred.await(handled);
      yield* Projection.run(projection);
      expect(calls).toBe(1);
      expect(yield* readAll(output)).toEqual([1]);
      expect(
        Option.isSome((yield* (yield* CheckpointStore).load(Projection.key(projection))).record),
      ).toBe(true);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("changes during an in-flight run coalesce to one follow-up run", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-coalesce-input", {
        schema: Schema.Finite,
      });
      const output = FamilyFixture.json("change-coalesce-output", {
        schema: Schema.Finite,
      });
      yield* Streams.create(input);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const second = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.mapAccumBatch({
        commit: Commit.producer,
        id: "change-coalesce",
        params: {},
        inputs: { input },
        outputs: { output: Output.stream(Schema.Finite, { stream: output }) },
        state: { schema: Schema.Null, initial: null },
        mapAccumBatch: (_state, batch) =>
          Effect.gen(function* () {
            calls += 1;
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            } else yield* Deferred.succeed(second, undefined);
            return [null, { output: batch.input.items }] as const;
          }),
      }).at({});
      const fiber = yield* Projection.watch(projection);
      yield* Streams.append(input, [1]);
      yield* Deferred.await(entered);
      yield* Streams.append(input, [2]);
      yield* Streams.append(input, [3]);
      yield* Streams.append(input, [4]);
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(second);
      yield* Projection.run(projection);
      expect(calls).toBe(2);
      expect(yield* readAll(output)).toEqual([1, 2, 3, 4]);
      const tail = yield* Streams.head(input);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(fiber);
    }),
  ));

test("a limited wake drains the backlog before waiting for another change", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-limit-input", {
        schema: Schema.Finite,
      });
      yield* Streams.create(input);
      yield* Streams.append(input, [1, 2, 3]);
      const seen: Array<number> = [];
      const drained = yield* Deferred.make<void>();
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "change-limit",
        params: {},
        inputs: { input },
        process: (batch) =>
          Effect.gen(function* () {
            seen.push(...batch.input.items);
            if (seen.length === 3) yield* Deferred.succeed(drained, undefined);
          }),
      }).at({});
      const fiber = yield* Projection.watch(projection, { limit: 1 });
      yield* Deferred.await(drained);
      yield* Projection.run(projection);
      expect(seen).toEqual([1, 2, 3]);
      const tail = yield* Streams.head(input);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(fiber);
    }).pipe(
      Effect.scoped,
      Effect.provide(FamilyFixture.withFamilies(layerMemory({ readLimit: 1 }))),
    ),
  ));

test("an invalid limit fails before a watcher fiber is created", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-invalid-limit", {
        schema: Schema.Finite,
      });
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "change-invalid-limit",
        params: {},
        inputs: { input },
        process: () => Effect.void,
      }).at({});
      const result = yield* Projection.watch(projection, { limit: 0 }).pipe(Effect.result);
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
      const input = FamilyFixture.json("change-probe-failure", { schema: Schema.Finite });
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "change-probe-failure",
        params: {},
        inputs: { input },
        process: () => Effect.void,
      }).at({});
      const reader = yield* StreamsReader;
      const fiber = yield* Projection.watch(projection).pipe(
        FamilyFixture.provideReader({
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

test("watch and a serialized request share the same key lock", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-request-input", {
        schema: Schema.Finite,
      });
      const output = FamilyFixture.json("change-request-output", {
        schema: Schema.Finite,
      });
      yield* Streams.create(input);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const projection = Projection.emit({
        commit: Commit.producer,
        id: "change-request",
        params: {},
        inputs: { input },
        outputs: { output: Output.stream(Schema.Finite, { stream: output }) },
        emit: (entry) =>
          Effect.sync(() => (calls += 1)).pipe(
            Effect.andThen(Deferred.succeed(entered, undefined)),
            Effect.andThen(Deferred.await(release)),
            Effect.as({ output: [entry.item] }),
          ),
      }).at({});
      const watcher = yield* Projection.watch(projection);
      yield* Streams.append(input, [1]);
      yield* Deferred.await(entered);
      const request = yield* Projection.run(projection).pipe(Effect.forkScoped);
      expect(request.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(request)).status).toBe("caught-up");
      expect(calls).toBe(1);
      expect(yield* readAll(output)).toEqual([1]);
      yield* Fiber.interrupt(watcher);
    }),
  ));

test("a stream watches its fixed address set independently", () =>
  run(
    Effect.gen(function* () {
      const seen: Array<string> = [];
      const a = yield* Deferred.make<void>();
      const b = yield* Deferred.make<void>();
      const declared = Projection.process({
        commit: Commit.transaction,
        id: "change-stream",
        params: { workspaceId: Schema.String },
        inputs: { facts: WatchRoute1 },
        process: (_batch, unit) =>
          Effect.gen(function* () {
            const workspaceId = unit.params.workspaceId;
            seen.push(workspaceId);
            yield* Deferred.succeed(workspaceId === "a" ? a : b, undefined);
          }),
      });
      const aRef = WatchRoute1.at({ workspaceId: "a" });
      const bRef = WatchRoute1.at({ workspaceId: "b" });
      yield* Streams.create(aRef);
      yield* Streams.create(bRef);
      const fibers = yield* Effect.forEach(["a", "b"], (workspaceId) =>
        Projection.watch(declared.at({ workspaceId })),
      );
      yield* Streams.append(aRef, [1]);
      yield* Deferred.await(a);
      expect(seen).toEqual(["a"]);
      yield* Streams.append(bRef, [2]);
      yield* Deferred.await(b);
      expect(seen).toEqual(["a", "b"]);
      yield* Effect.forEach(fibers, Fiber.interrupt);
    }),
  ));

test("closing the caller scope interrupts every stream address fiber", () =>
  run(
    Effect.gen(function* () {
      const declared = Projection.process({
        commit: Commit.transaction,
        id: "change-stream-scope",
        params: { workspaceId: Schema.String },
        inputs: { facts: WatchRoute2 },
        process: () => Effect.void,
      });
      const fibers = yield* Effect.scoped(
        Effect.forEach(["a", "b"], (workspaceId) => Projection.watch(declared.at({ workspaceId }))),
      );
      for (const fiber of fibers) expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
    }),
  ));

test("a closed and drained input ends its watcher", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-closed", { schema: Schema.Finite });
      yield* Streams.create(input);
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "change-closed",
        params: {},
        inputs: { input },
        process: () => Effect.void,
      }).at({});
      const fiber = yield* Projection.watch(projection);
      yield* Streams.append(input, [1]);
      yield* Streams.append(input, [], { close: true });
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
    }),
  ));

test("closing the caller scope ends the watcher fiber", () =>
  run(
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-scope", { schema: Schema.Finite });
      yield* Streams.create(input);
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "change-scope",
        params: {},
        inputs: { input },
        process: () => Effect.void,
      }).at({});
      const fiber = yield* Effect.scoped(Projection.watch(projection));
      expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true);
    }),
  ));
