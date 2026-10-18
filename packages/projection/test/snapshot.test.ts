import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { Commit, Transaction } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
import { expect } from "vitest";
import { Effect, FileSystem, Path, Layer, Option, Schema } from "effect";
import { ZERO_OFFSET, type StorageFault, type StreamAddress } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, type ProjectionFault } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import { BunStorage } from "@streamsy/storage/bun";

const input = FamilyFixture.json("snapshot-input", { schema: Schema.Finite });

const words = FamilyFixture.json("snapshot-words", { schema: Schema.String });

const Totals = Schema.Struct({ sum: Schema.Finite, count: Schema.Finite });

const totals = Projection.fold({
  id: "snapshot-totals",
  params: {},
  inputs: { input, words },
  state: { schema: Totals, initial: { sum: 0, count: 0 } },
  fold: (state, entry) => ({
    sum: state.sum + (entry.input === "input" ? entry.item : 0),
    count: state.count + 1,
  }),
}).at({});

const seed = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.create(words);
  yield* Streams.append(input, [2, 3]);
  yield* Streams.append(words, ["a"]);
});

const tailOf = (address: StreamAddress.StreamAddress<unknown, unknown, unknown>) =>
  Streams.head(address).pipe(Effect.map((head) => head.nextOffset));

const sqlite = (filename = ":memory:") =>
  FamilyFixture.withFamilies(
    Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })),
    ),
  );

const hosts: ReadonlyArray<
  readonly [
    string,
    Layer.Layer<
      CheckpointStore | import("@streamsy/core").Streamsy.Wildcard | StreamsReader | StreamsWriter,
      StorageFault | ProjectionFault
    >,
  ]
> = [
  ["memory", FamilyFixture.withFamilies(memoryHost())],
  ["SQLite", sqlite()],
];

for (const [name, host] of hosts) {
  layer(host)((it) => {
    it.effect(`${name}: read before the first unit gives the initial state and start offsets`, () =>
      Effect.gen(function* () {
        expect(yield* Projection.read(totals)).toEqual({
          state: { sum: 0, count: 0 },
          offsets: { input: ZERO_OFFSET, words: ZERO_OFFSET },
        });
        expect(
          Option.flatMap((yield* (yield* CheckpointStore).load(totals)).record, (record) =>
            Option.fromUndefinedOr(record.state),
          ),
        ).toEqual(Option.none());
      }),
    );
  });

  layer(host)((it) => {
    it.effect(
      `${name}: read gives the state and offsets of the last unit, as run reports them`,
      () =>
        Effect.gen(function* () {
          yield* seed;
          const progress = yield* Projection.run(totals);
          expect(progress.status).toBe("caught-up");
          const store = yield* CheckpointStore;
          let loads = 0;

          const snapshot = yield* Projection.read(totals).pipe(
            Effect.provideService(CheckpointStore, {
              ...store,
              load: (key) =>
                Effect.suspend(() => {
                  loads += 1;

                  return store.load(key);
                }),
            }),
          );

          // State and offsets come from one record load.
          expect(loads).toBe(1);
          expect(snapshot.offsets).toEqual(progress.offsets);
          expect(snapshot.offsets).toEqual({
            input: yield* tailOf(input),
            words: yield* tailOf(words),
          });
          expect(snapshot.state).toEqual({ sum: 5, count: 3 });
        }),
    );
  });

  layer(host)((it) => {
    it.effect(
      `${name}: an outputs handler gets the prior state and its returned state is saved`,
      () =>
        Effect.gen(function* () {
          const doubled = FamilyFixture.json("snapshot-doubled", { schema: Schema.Finite });
          const seen: number[] = [];

          const counter = Projection.mapAccumBatch({
            commit: Commit.producer,
            id: "snapshot-counter",
            params: { start: Schema.FiniteFromString },
            inputs: { input },
            outputs: { doubled: doubled },
            state: { schema: Schema.Finite, initial: ({ start }) => start },
            mapAccumBatch: (count, batch) =>
              Effect.sync(() => {
                seen.push(count);

                return [
                  count + batch.input.items.length,
                  { doubled: batch.input.items.map((n) => n * 2) },
                ] as const;
              }),
          }).at({ start: 10 });

          yield* seed;
          expect((yield* Projection.read(counter)).state).toBe(10);
          const first = yield* Projection.run(counter);
          yield* Streams.append(input, [7]);
          yield* Projection.run(counter);
          const snapshot = yield* Projection.read(counter);
          expect(seen).toEqual([10, 12]);
          expect(snapshot.state).toBe(13);
          expect(snapshot.offsets.input).not.toBe(first.offsets.input);
          expect(snapshot.offsets.input).toBe(yield* tailOf(input));
        }),
    );
  });

  layer(host)((it) => {
    it.effect(
      `${name}: read of a projection without state: gives undefined state and its offsets`,
      () =>
        Effect.gen(function* () {
          yield* seed;

          const stateless = Projection.process({
            commit: Commit.atLeastOnce,
            id: "snapshot-stateless",
            params: {},
            inputs: { input },
            process: () => Effect.void,
          }).at({});

          const progress = yield* Projection.run(stateless);
          expect(yield* Projection.read(stateless)).toEqual({
            state: undefined,
            offsets: progress.offsets,
          });
        }),
    );
  });
}

layer(BunServices.layer)((it) => {
  it.effect("SQLite: a second process writes between two reads of the same file", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "projection-snapshot-" });
      const filename = path.join(directory, "snapshot.sqlite");
      const reader = yield* Layer.build(sqlite(filename));
      const writer = yield* Layer.build(sqlite(filename));
      yield* seed.pipe(Effect.andThen(Projection.run(totals)), Effect.provideContext(reader));
      const first = yield* Projection.read(totals).pipe(Effect.provideContext(reader));
      yield* Streams.append(words, ["b", "c"]).pipe(
        Effect.andThen(Projection.run(totals)),
        Effect.provideContext(writer),
      );
      const second = yield* Projection.read(totals).pipe(Effect.provideContext(reader));
      expect(first.state).toEqual({ sum: 5, count: 3 });
      expect(second.state).toEqual({ sum: 5, count: 5 });
      expect(second.offsets.input).toBe(first.offsets.input);
      expect(second.offsets.words).not.toBe(first.offsets.words);
      expect(second.offsets.words).toBe(yield* tailOf(words).pipe(Effect.provideContext(reader)));
    }),
  );
});
