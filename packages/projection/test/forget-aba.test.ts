import { Transaction } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "vitest";
import { Deferred, Effect, Fiber, Layer, Schema } from "effect";
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
  test(`${name}: forget while a writer runs can reuse a token`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const input = FamilyFixture.json("aba-input", { schema: Schema.Finite });

        const projection = Projection.fold({
          id: `aba-${name}`,
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

        const a = yield* Projection.run(projection).pipe(
          FamilyFixture.provideReader(slow),
          Effect.forkChild,
        );

        yield* Deferred.await(reading);
        const started = yield* Deferred.make<void>();

        const forget = yield* Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Projection.forget(projection)),
          Effect.forkChild,
        );

        yield* Deferred.await(started);

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
      }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
    ));
}
