/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "bun:test";
import { Deferred, Effect, Fiber, Layer, Option, Schema } from "effect";
import { StreamRef, Streams, StreamsReader, type StorageFault } from "@streamsy/core";
import { Checkpoints, Projection, type Host, type ProjectionFault } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";

const hosts: ReadonlyArray<
  readonly [string, Layer.Layer<Host | Projection.State, StorageFault | ProjectionFault>]
> = [
  ["memory", Memory.layerMemory()],
  [
    "SQLite",
    Sqlite.layer.pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
];
for (const [name, host] of hosts) {
  for (const serialized of [true, false]) {
    test(`${name}: forget ${serialized ? "waits for the serialized runner, preventing ABA" : "cannot protect a bare run: caller must drain it first"}`, () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const input = StreamRef.json("aba-input", { schema: Schema.Finite });
          const projection = Projection.make({
            id: `aba-${name}-${serialized}`,
            input,
            process: Projection.fold(Schema.Finite, 0, (sum, { item }) => sum + item),
          });
          yield* Streams.create(input);
          yield* Streams.append(input, [2, 3]);
          yield* Projection.serialized(projection);
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
          const a = yield* (
            serialized ? Projection.serialized(projection) : Projection.run(projection)
          ).pipe(Effect.provideService(StreamsReader, slow), Effect.forkChild);
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
            expect((yield* (yield* Checkpoints).load(projection)).token).toBe("1");
            yield* Deferred.succeed(release, undefined);
            expect((yield* Fiber.join(a)).items).toBe(1);
            yield* Fiber.join(forget);
            expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.none());
            yield* Projection.serialized(projection);
            expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(12));
          } else {
            yield* Fiber.join(forget);
            yield* Projection.serialized(projection);
            expect((yield* (yield* Checkpoints).load(projection)).token).toBe("1");
            expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(12));
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(a);
            // The old token 1 matches the fresh token 1: this is the remaining caller rule.
            expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(19));
          }
        }).pipe(Effect.provide(host)),
      ));
  }
}

test("forget retains the shared semaphore while an existing waiter starts its run", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const input = StreamRef.json("queued-input", { schema: Schema.Finite });
      const projection = Projection.make({
        id: "queued-forget",
        input,
        process: () => Effect.void,
      });
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
      const first = yield* Projection.serialized(projection).pipe(
        Effect.provideService(StreamsReader, gated(firstRead, firstRelease)),
        Effect.forkChild,
      );
      yield* Deferred.await(firstRead);
      const forget = yield* Projection.forget(projection).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const second = yield* Projection.serialized(projection).pipe(
        Effect.provideService(StreamsReader, gated(secondRead, secondRelease)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* Deferred.succeed(firstRelease, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(forget);
      yield* Deferred.await(secondRead);
      let thirdReads = 0;
      const third = yield* Projection.serialized(projection).pipe(
        Effect.provideService(StreamsReader, {
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
    }).pipe(Effect.provide(Memory.layerMemory())),
  ));
