import { BunStorage } from "@streamsy/storage/bun";
import { CommitBoundary, StorageFault } from "@streamsy/core";
import { Streams, StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { expect, layer } from "@streamsy/effect-test";
import { Deferred, Effect, Fiber, Layer, Option, Schema } from "effect";
import * as Families from "./support/families.ts";
import { memoryHost } from "./support/memory-host.ts";

const input = Families.json("current-input", { schema: Schema.Finite });

const output = Families.json("current-output", { schema: Schema.Finite });

const total = Projection.fold({
  id: "current-total",
  params: {},
  inputs: { input },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (sum, { item }) => sum + item,
}).at({});

const hosts = [
  ["memory", memoryHost({ readLimit: 1 })],
  [
    "SQLite",
    CheckpointStore.layer.pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
] as const;

for (const [name, host] of hosts) {
  layer(Families.withFamilies(host))((it) =>
    it.effect(`${name}: current reads the tail without a follower or checkpoint writes`, () =>
      Effect.gen(function* () {
        yield* Streams.create(input);
        const store = yield* CheckpointStore;
        const absent = yield* store.load(total);
        expect((yield* Projection.current(total)).state).toBe(0);
        yield* Streams.append(input, [2, 3]);
        expect((yield* Projection.current(total)).state).toBe(5);
        expect(yield* store.load(total)).toEqual(absent);
        yield* Projection.run(total);
        const saved = yield* store.load(total);
        const appended = yield* Streams.append(input, [7]);
        expect(yield* Projection.current(total)).toEqual({
          state: 12,
          offsets: { input: appended.offset },
        });
        expect(yield* store.load(total)).toEqual(saved);
        expect((yield* Projection.read(total)).state).toBe(5);
      }),
    ),
  );

  layer(Families.withFamilies(host))((it) =>
    it.effect(`${name}: stateful output adapters discard outputs and include pending ranges`, () =>
      Effect.gen(function* () {
        yield* Streams.create(input);
        yield* Streams.create(output);
        yield* Streams.append(input, [2, 3]);

        const accum = Projection.mapAccum({
          id: "current-accum",
          params: {},
          inputs: { input },
          outputs: { output },
          commit: Commit.producer,
          state: { schema: Schema.Finite, initial: 0 },
          mapAccum: (sum, { item }) => [sum + item, { output: [item] }] as const,
        }).at({});

        // A failed output append leaves a pinned unit with committed offsets at zero.
        const store = yield* CheckpointStore;
        const writer = yield* StreamsWriter;
        expect(
          (yield* Projection.run(accum).pipe(
            Families.provideWriter({
              ...writer,
              append: () =>
                Effect.fail(
                  new StorageFault({
                    operation: "append",
                    message: "crash after pin",
                    retryable: false,
                  }),
                ),
            }),
            Effect.result,
          ))._tag,
        ).toBe("Failure");
        const pinned = yield* store.load(accum);
        expect(Option.getOrThrow(pinned.record).pending).toBeDefined();
        expect((yield* Projection.current(accum)).state).toBe(5);
        expect(yield* store.load(accum)).toEqual(pinned);
        expect((yield* Streams.collect(output)).items).toEqual([]);

        const batch = Projection.mapAccumBatch({
          id: "current-batch",
          params: {},
          inputs: { input },
          outputs: { output },
          commit: Commit.producer,
          state: { schema: Schema.Finite, initial: 0 },
          mapAccumBatch: (sum, slices) =>
            [sum + slices.input.items.reduce((a, b) => a + b, 0), { output: [99] }] as const,
        }).at({});

        expect((yield* Projection.current(batch)).state).toBe(5);
        expect((yield* Streams.collect(output)).items).toEqual([]);
      }),
    ),
  );

  layer(Families.withFamilies(host))((it) =>
    it.effect(`${name}: twenty concurrent decisions append or lose only an offset race`, () =>
      Effect.gen(function* () {
        yield* Streams.create(input);
        yield* Streams.append(input, [1]);

        const results = yield* Effect.forEach(
          Array.from({ length: 20 }, (_, index) => index),
          () =>
            Projection.decide(total, {
              input: "input",
              decide: ({ state }) => Effect.yieldNow.pipe(Effect.as({ value: state, items: [1] })),
            }).pipe(Effect.result),
          { concurrency: "unbounded" },
        );

        for (const result of results) {
          if (result._tag === "Failure") expect(result.failure._tag).toBe("OffsetMismatch");
          else expect(result.success.appended).toBe(true);
        }

        const fresh = yield* Streams.collect(input);
        expect((yield* Projection.current(total)).state).toBe(
          fresh.items.reduce((a, b) => a + b, 0),
        );
        expect((yield* (yield* CheckpointStore).load(total)).record).toEqual(Option.none());
      }),
    ),
  );

  layer(Families.withFamilies(host))((it) =>
    it.effect(`${name}: two writers on a key fail with an explanatory token conflict`, () =>
      Effect.gen(function* () {
        yield* Streams.create(input);
        yield* Streams.append(input, [1]);
        yield* Projection.run(total);
        yield* Streams.append(input, [2]);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        const slow = {
          ...total,
          process: (...args: Parameters<typeof total.process>) =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(total.process(...args)),
            ),
        };

        const writer = yield* Projection.run(slow).pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(entered);
        yield* Projection.run(total);
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(writer);
        expect(result._tag).toBe("Failure");

        if (result._tag === "Failure") {
          expect(result.failure).toMatchObject({ reason: "token-conflict" });
          expect(result.failure.message).toContain("second writer on the same key");
        }
      }),
    ),
  );

  layer(Families.withFamilies(host))((it) =>
    it.effect(`${name}: batch B follower and decisions inside the owner finish`, () =>
      Effect.gen(function* () {
        yield* Streams.create(input);
        yield* Streams.append(input, [1]);
        yield* Projection.run(total);
        yield* Streams.append(input, [2]);
        const reading = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const reader = yield* StreamsReader;
        let first = true;

        const follower = yield* Projection.follow(total, { repairIntervalMs: 10 }).pipe(
          Families.provideReader({
            ...reader,
            read: (id, options) =>
              Effect.gen(function* () {
                if (first) {
                  first = false;
                  yield* Deferred.succeed(reading, undefined);
                  yield* Deferred.await(release);
                }

                return yield* reader.read(id, options);
              }),
          }),
        );

        yield* Deferred.await(reading);
        const boundary = yield* CommitBoundary;
        yield* boundary.withTransaction(
          Effect.gen(function* () {
            yield* Deferred.succeed(release, undefined);

            for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
            yield* Projection.decide(total, {
              input: "input",
              decide: ({ state }) => Effect.succeed({ value: state, items: [1] }),
            });
          }),
        );
        yield* Effect.forEach(
          Array.from({ length: 19 }),
          () =>
            boundary.withTransaction(
              Projection.decide(total, {
                input: "input",
                decide: ({ state }) => Effect.succeed({ value: state, items: [1] }),
              }),
            ),
          { concurrency: "unbounded" },
        );
        yield* Streams.append(input, [0], { close: true });
        expect((yield* Fiber.join(follower)).status).toBe("source-closed");
        expect((yield* Projection.current(total)).state).toBe(23);
      }).pipe(Effect.timeout("5 seconds")),
    ),
  );
}
