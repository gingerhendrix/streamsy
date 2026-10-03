import { Commit, Transaction } from "@streamsy/projection";
import { runKernel } from "../src/run.ts";
import * as FamilyFixture from "./support/families.ts";
/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Layer, Option, Schema } from "effect";
import { type StorageFault } from "@streamsy/core";
import { Streams, StreamsReader } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, type ProjectionFault } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import { BunStorage } from "@streamsy/storage/bun";

const hosts: ReadonlyArray<
  readonly [
    string,
    Layer.Layer<
      | CheckpointStore
      | import("@streamsy/core/protocol").StreamsReader
      | import("@streamsy/core/protocol").StreamsWriter,
      StorageFault | ProjectionFault
    >,
  ]
> = [
  ["memory", memoryHost()],
  [
    "SQLite",
    Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
];
for (const [name, host] of hosts) {
  for (const serialized of [true, false]) {
    test(`${name}: forget ${serialized ? "waits for the serialized runner, preventing ABA" : "cannot protect an internal kernel run simulating another process"}`, () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const input = FamilyFixture.json("aba-input", { schema: Schema.Finite });
          const projection = Projection.fold({
            id: `aba-${name}-${serialized}`,
            params: {},
            inputs: { input },
            state: { schema: Schema.Finite, initial: 0 },
            fold: (sum, { item }) => sum + item,
          }).at({});
          yield* Streams.create(input);
          yield* Streams.append(input, [2, 3]);
          yield* Projection.run(projection);
          yield* Streams.append(input, [7]);
          const reading = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const reader = yield* StreamsReader;
          let first = true;
          const slow = {
            ...reader,
            read: (
              id: Parameters<typeof reader.read>[0],
              options: Parameters<typeof reader.read>[1],
            ) =>
              Effect.gen(function* () {
                if (first) {
                  first = false;
                  yield* Deferred.succeed(reading, undefined);
                  yield* Deferred.await(release);
                }
                return yield* reader.read(id, options);
              }),
          };
          const a = yield* (serialized ? Projection.run(projection) : runKernel(projection)).pipe(
            FamilyFixture.provideReader(slow),
            Effect.forkChild,
          );
          yield* Deferred.await(reading);
          let forgotten = false;
          const started = yield* Deferred.make<void>();
          const forget = yield* Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Projection.forget(projection)),
            Effect.tap(() =>
              Effect.sync(() => {
                forgotten = true;
              }),
            ),
            Effect.forkChild,
          );
          yield* Deferred.await(started);
          if (serialized) {
            for (let n = 0; n < 10; n += 1) yield* Effect.yieldNow;
            expect(forgotten).toBe(false);
            expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("1");
            yield* Deferred.succeed(release, undefined);
            expect((yield* Fiber.join(a)).items).toBe(1);
            yield* Fiber.join(forget);
            expect(
              Option.flatMap((yield* (yield* CheckpointStore).load(projection)).record, (record) =>
                Option.fromUndefinedOr(record.state),
              ),
            ).toEqual(Option.none());
            yield* Projection.run(projection);
            expect((yield* Projection.read(projection)).state).toBe(12);
          } else {
            yield* Fiber.join(forget);
            yield* Projection.run(projection);
            expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("1");
            expect((yield* Projection.read(projection)).state).toBe(12);
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(a);
            // The stale pass still commits against a reused token. It now folds the state
            // loaded with its offsets, so this example overwrites with the same value.
            // Another process must still be drained separately before forget.
            expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("2");
            expect((yield* Projection.read(projection)).state).toBe(12);
          }
        }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
      ));
  }
}

test("forget retains the shared semaphore while an existing waiter starts its run", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const input = FamilyFixture.json("queued-input", { schema: Schema.Finite });
      const projection = Projection.process({
        commit: Commit.atLeastOnce,
        id: "queued-forget",
        params: {},
        inputs: { input },
        process: () => Effect.void,
      }).at({});
      yield* Streams.create(input);
      yield* Streams.append(input, [1]);
      const reader = yield* StreamsReader;
      const firstRead = yield* Deferred.make<void>();
      const firstRelease = yield* Deferred.make<void>();
      const secondRead = yield* Deferred.make<void>();
      const secondRelease = yield* Deferred.make<void>();
      const gated = (read: Deferred.Deferred<void>, release: Deferred.Deferred<void>) => ({
        ...reader,
        read: (id: Parameters<typeof reader.read>[0], options: Parameters<typeof reader.read>[1]) =>
          Deferred.succeed(read, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(reader.read(id, options)),
          ),
      });
      const first = yield* Projection.run(projection).pipe(
        FamilyFixture.provideReader(gated(firstRead, firstRelease)),
        Effect.forkChild,
      );
      yield* Deferred.await(firstRead);
      const forget = yield* Projection.forget(projection).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const second = yield* Projection.run(projection).pipe(
        FamilyFixture.provideReader(gated(secondRead, secondRelease)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* Deferred.succeed(firstRelease, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(forget);
      yield* Deferred.await(secondRead);
      let thirdReads = 0;
      const third = yield* Projection.run(projection).pipe(
        FamilyFixture.provideReader({
          ...reader,
          read: (id, options) =>
            Effect.suspend(() => {
              thirdReads += 1;
              return reader.read(id, options);
            }),
        }),
        Effect.forkChild,
      );
      for (let n = 0; n < 10; n += 1) yield* Effect.yieldNow;
      expect(thirdReads).toBe(0);
      yield* Deferred.succeed(secondRelease, undefined);
      expect((yield* Fiber.join(second)).items).toBe(1);
      expect((yield* Fiber.join(third)).items).toBe(0);
    }).pipe(Effect.provide(FamilyFixture.withFamilies(memoryHost()))),
  ));
