import { layer } from "@streamsy/effect-test";
import { Commit } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { Streamsy } from "@streamsy/core";
import { expect } from "bun:test";
import { Array, Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect";
import { StreamGone, StreamAddress } from "@streamsy/core";
import { Streams, StreamsReader } from "@streamsy/core/protocol";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { layerSqlite } from "./support/sqlite.ts";
import { memoryHost } from "./support/memory-host.ts";

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

/** Wait for the watcher checkpoint without starting another writer. */
const awaitCheckpoint = (projection: Parameters<typeof Projection.key>[0], offset: string) =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;

    while (true) {
      const loaded = yield* store.load(Projection.key(projection));

      if (Option.isSome(loaded.record) && loaded.record.value.inputs.input === offset) return;
      yield* Effect.yieldNow;
    }
  });

const readAll = <A>(address: StreamAddress.StreamAddress<A>) =>
  Streams.read(address).pipe(Streams.items, Stream.runCollect);

layer(FamilyFixture.withFamilies(layerSqlite()))((it) => {
  it.effect(
    "an append runs an transactional projection and commits its write with the checkpoint",
    () =>
      Effect.gen(function* () {
        const input = FamilyFixture.json("change-transactional-input", {
          schema: Schema.Finite,
        });

        const output = FamilyFixture.json("change-transactional-output", {
          schema: Schema.Finite,
        });

        yield* Streams.create(input);
        yield* Streams.create(output);
        const handled = yield* Deferred.make<void>();
        let calls = 0;

        const projection = Projection.process({
          commit: Commit.transaction,
          id: "change-transactional",
          params: {},
          inputs: { input },
          process: (batch) =>
            Effect.gen(function* () {
              const items = batch.input.items;

              if (Array.isReadonlyArrayNonEmpty(items)) yield* Streams.append(output, items);
              calls += 1;
              yield* Deferred.succeed(handled, undefined);
            }),
        }).at({});

        const fiber = yield* Projection.watch(projection);
        yield* Streams.append(input, [1]);
        yield* Deferred.await(handled);
        yield* awaitCheckpoint(projection, (yield* Streams.head(input)).nextOffset);
        expect(calls).toBe(1);
        expect(yield* readAll(output)).toEqual([1]);
        expect(
          Option.isSome((yield* (yield* CheckpointStore).load(Projection.key(projection))).record),
        ).toBe(true);
        yield* Fiber.interrupt(fiber);
      }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), WatchRoute1, WatchRoute2))((it) => {
  it.effect("changes during an in-flight run coalesce to one follow-up run", () =>
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
        outputs: { output: output },
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
      yield* awaitCheckpoint(projection, (yield* Streams.head(input)).nextOffset);
      expect(calls).toBe(2);
      expect(yield* readAll(output)).toEqual([1, 2, 3, 4]);
      const tail = yield* Streams.head(input);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(fiber);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost({ readLimit: 1 })))((it) => {
  it.effect("a limited wake drains the backlog before waiting for another change", () =>
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-limit-input", {
        schema: Schema.Finite,
      });

      yield* Streams.create(input);
      yield* Streams.append(input, [1, 2, 3]);
      const seen: number[] = [];
      const drained = yield* Deferred.make<void>();

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
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
      yield* awaitCheckpoint(projection, (yield* Streams.head(input)).nextOffset);
      expect(seen).toEqual([1, 2, 3]);
      const tail = yield* Streams.head(input);
      const loaded = yield* (yield* CheckpointStore).load(Projection.key(projection));
      expect(Option.getOrThrow(loaded.record).inputs.input).toBe(tail.nextOffset);
      yield* Fiber.interrupt(fiber);
    }),
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), WatchRoute1, WatchRoute2))((it) => {
  it.effect("an invalid limit fails before a watcher fiber is created", () =>
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-invalid-limit", {
        schema: Schema.Finite,
      });

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
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
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), WatchRoute1, WatchRoute2))((it) => {
  it.effect("an ownership probe reports a gone input as unavailable history", () =>
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-probe-failure", { schema: Schema.Finite });

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
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
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), WatchRoute1, WatchRoute2))((it) => {
  it.effect("a stream watches its fixed address set independently", () =>
    Effect.gen(function* () {
      const seen: string[] = [];
      const a = yield* Deferred.make<void>();
      const b = yield* Deferred.make<void>();

      const declared = Projection.process({
        commit: Commit.atLeastOnce,
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
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), WatchRoute1, WatchRoute2))((it) => {
  it.effect("closing the caller scope interrupts every stream address fiber", () =>
    Effect.gen(function* () {
      const declared = Projection.process({
        commit: Commit.atLeastOnce,
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
  );
});

layer(FamilyFixture.withFamilies(memoryHost(), WatchRoute1, WatchRoute2))((it) => {
  it.effect("a closed and drained input ends its watcher", () =>
    Effect.gen(function* () {
      const input = FamilyFixture.json("change-closed", { schema: Schema.Finite });
      yield* Streams.create(input);

      const projection = Projection.process({
        commit: Commit.atLeastOnce,
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
  );
});
