import { Commit, Transaction } from "@streamsy/projection";
import { tmpdir } from "node:os";
import { restore } from "../src/checkpoint-store.ts";
import { runKernel } from "../src/run.ts";
import * as FamilyFixture from "./support/families.ts";
/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Effect, Layer, ManagedRuntime, Option, Schema } from "effect";
import { ZERO_OFFSET, type StorageFault, type StreamAddress } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, type ProjectionFault } from "@streamsy/projection";
import * as Memory from "./support/memory.ts";
import * as BunStorage from "@streamsy/storage/bun";

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
  ["memory", FamilyFixture.withFamilies(Memory.layerMemory())],
  ["SQLite", sqlite()],
];

for (const [name, host] of hosts) {
  test(`${name}: read before the first unit gives the initial state and start offsets`, () =>
    Effect.runPromise(
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
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: read gives the state and offsets of the last unit, as run reports them`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const progress = yield* Projection.run(totals);
        expect(progress.status).toBe("caught-up");
        const snapshot = yield* Projection.read(totals);
        expect(snapshot.offsets).toEqual(progress.offsets);
        expect(snapshot.offsets).toEqual({
          input: yield* tailOf(input),
          words: yield* tailOf(words),
        });
        expect(snapshot.state).toEqual({ sum: 5, count: 3 });
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: state and offsets move together when another writer runs between two reads`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(totals);
        const first = yield* Projection.read(totals);
        yield* Streams.append(input, [7]);
        // A kernel run without the permit stands in for another process.
        yield* runKernel(totals);
        const second = yield* Projection.read(totals);
        expect(second.offsets.input).not.toBe(first.offsets.input);
        expect(second.offsets.words).toBe(first.offsets.words);
        expect(second.offsets.input).toBe(yield* tailOf(input));
        expect(first.state).toEqual({ sum: 5, count: 3 });
        expect(second.state).toEqual({ sum: 12, count: 4 });
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: read loads state and offsets in one record`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        yield* Projection.run(totals);
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
        expect(loads).toBe(1);
        expect(snapshot.state).toEqual({ sum: 5, count: 3 });
        expect(Option.getOrThrow((yield* store.load(totals)).record).state).toBe(
          '{"sum":5,"count":3}',
        );
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: an outputs handler gets the prior state and its returned state is saved`, () =>
    Effect.runPromise(
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
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: read of a projection without state: gives undefined state and its offsets`, () =>
    Effect.runPromise(
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
      }).pipe(Effect.provide(host)),
    ));
}

test("read of an invalid stored state fails as an invalid record", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const checkpoints = yield* CheckpointStore;
      const before = yield* restore(totals, checkpoints);
      yield* checkpoints.compareAndSet(totals, before.token, {
        identity: before.record.identity,
        inputs: before.record.inputs,
        adapters: before.record.adapters,
        state: '"not totals"',
      });
      const failed = yield* Projection.read(totals).pipe(Effect.result);
      expect(failed._tag).toBe("Failure");
      if (failed._tag === "Failure")
        expect(failed.failure).toMatchObject({ phase: "load", reason: "invalid-record" });
    }).pipe(Effect.provide(FamilyFixture.withFamilies(Memory.layerMemory()))),
  ));

test("SQLite: a second process writes between two reads of the same file", async () => {
  const directory = mkdtempSync(join(tmpdir(), "projection-snapshot-"));
  const filename = join(directory, "snapshot.sqlite");
  const reader = ManagedRuntime.make(sqlite(filename));
  const writer = ManagedRuntime.make(sqlite(filename));
  try {
    await reader.runPromise(seed.pipe(Effect.andThen(Projection.run(totals))));
    const first = await reader.runPromise(Projection.read(totals));
    await writer.runPromise(
      Streams.append(words, ["b", "c"]).pipe(Effect.andThen(Projection.run(totals))),
    );
    const second = await reader.runPromise(Projection.read(totals));
    expect(first.state).toEqual({ sum: 5, count: 3 });
    expect(second.state).toEqual({ sum: 5, count: 5 });
    expect(second.offsets.input).toBe(first.offsets.input);
    expect(second.offsets.words).not.toBe(first.offsets.words);
    expect(second.offsets.words).toBe(await reader.runPromise(tailOf(words)));
  } finally {
    await reader.dispose();
    await writer.dispose();
    rmSync(directory, { recursive: true });
  }
});
