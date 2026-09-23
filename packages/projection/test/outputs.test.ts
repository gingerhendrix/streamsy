/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "bun:test";
import { Effect, Layer, Option, Schema } from "effect";
import { StreamRef, Streams, StreamsWriter, TransportFault } from "@streamsy/core";
import { Checkpoints, Output, Projection, ProjectionFault } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { readAll } from "./support/scenarios.ts";

const hosts: ReadonlyArray<
  readonly [
    string,
    Layer.Layer<
      import("@streamsy/projection").Host | Projection.State,
      import("@streamsy/core").StorageFault | ProjectionFault
    >,
  ]
> = [
  ["memory", Memory.layerMemory()],
  [
    "SQLite",
    Sqlite.layer.pipe(
      Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
    ),
  ],
] as const;
const input = StreamRef.json("named-input", { schema: Schema.Finite });
const a = StreamRef.json("named-a", { schema: Schema.Finite });
const b = StreamRef.json("named-b", { schema: Schema.Finite });
const declarations = {
  a: Output.stream(Schema.Finite, { stream: a }),
  b: Output.stream(Schema.Finite, { stream: b }),
};
const seed = Streams.create(input).pipe(Effect.andThen(Streams.append(input, [1])));

const numbers = StreamRef.json("numbers", { schema: Schema.Finite });
const Card = Schema.Struct({ issueId: Schema.String, total: Schema.Finite });
const tracker = Projection.outputs({
  id: "tracker",
  inputs: { numbers },
  outputs: {
    board: Output.rows(Card, { key: "issueId", stream: "board" }),
    transitions: Output.stream(Schema.Finite, { stream: "transitions" }),
    summary: Output.value(Schema.Finite),
  },
  process: Projection.fold(0, (state, batch) => {
    const next = state + batch.numbers.items.reduce((sum, n) => sum + n, 0);
    return {
      state: next,
      board: [Output.upsert({ issueId: "total", total: next }), Output.remove("old")],
      transitions: [next],
    };
  }),
});

for (const [name, host] of hosts) {
  test(`${name}: the Outputs documentation snippet writes rows, events and state`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* Streams.create(numbers);
        yield* Streams.append(numbers, [1, 2, 3]);
        yield* Projection.run(tracker);
        expect(yield* Projection.loadState(tracker, Schema.Finite)).toEqual(Option.some(6));
        expect(yield* readAll(StreamRef.json("transitions", { schema: Schema.Finite }))).toEqual([
          6,
        ]);
        expect(yield* readAll(StreamRef.json("board", { schema: Schema.Unknown }))).toEqual([
          {
            type: "board",
            key: "total",
            value: { issueId: "total", total: 6 },
            headers: { operation: "upsert" },
          },
          { type: "board", key: "old", headers: { operation: "delete" } },
        ]);
      }).pipe(Effect.provide(host)),
    ));
  test(`${name}: independent sequences skip empty outputs and versions own their producer ids`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const projection = Projection.outputs({
          id: "named",
          inputs: { input },
          outputs: declarations,
          process: Projection.each(({ item }) =>
            Effect.succeed({ a: [item * 10], b: item % 2 === 0 ? [item] : [] }),
          ),
        });
        const saved: import("@streamsy/projection/checkpoint").CheckpointRecord[] = [];
        const owner = yield* Checkpoints;
        yield* Projection.run(projection).pipe(
          Effect.provideService(Checkpoints, {
            ...owner,
            save: (key, record, token) => {
              saved.push(record);
              return owner.save(key, record, token);
            },
          }),
        );
        // An owner may retain the passed record: settling must not mutate that pin.
        expect(saved[0]?.adapters.outputs?.a?.nextSeq).toBe(0);
        expect(saved[1]?.adapters.outputs?.a?.nextSeq).toBe(1);
        expect(yield* (yield* Checkpoints).load(projection)).toMatchObject({
          record: Option.some({
            identity: { inputs: expect.anything() },
            inputs: expect.anything(),
            adapters: { outputs: { a: { epoch: 1, nextSeq: 1 } } },
          }),
        });
        yield* Streams.append(input, [2]);
        yield* Projection.run(projection);
        expect(yield* readAll(a)).toEqual([10, 20]);
        expect(yield* readAll(b)).toEqual([2]);
        const ids: string[] = [];
        const writer = yield* StreamsWriter;
        const v2 = Projection.outputs({
          id: "named",
          version: 2,
          inputs: { input },
          outputs: declarations,
          process: (batch) => Effect.succeed({ a: batch.input.items.map((n) => n * 100), b: [] }),
        });
        yield* Projection.run(v2).pipe(
          Effect.provideService(StreamsWriter, {
            ...writer,
            append: (id, options) => {
              ids.push(options.producer?.producerId ?? "");
              return writer.append(id, options);
            },
          }),
        );
        expect(ids).toEqual(["named/v2"]);
        expect(yield* readAll(a)).toEqual([10, 20, 100, 200]);
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: a stop between appends replays from pre-unit state and settles once`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const states: number[] = [];
        const projection = Projection.outputs({
          id: "recover",
          inputs: { input },
          outputs: { ...declarations, total: Output.value(Schema.Finite) },
          process: Projection.fold(0, (state, batch) => {
            states.push(state);
            const next = state + batch.input.items.reduce((sum, n) => sum + n, 0);
            return { state: next, a: [next], b: [next] };
          }),
        });
        const writer = yield* StreamsWriter;
        const failed = yield* Projection.run(projection).pipe(
          Effect.provideService(StreamsWriter, {
            ...writer,
            append: (id, options) =>
              id === b.id
                ? Effect.fail(
                    new TransportFault({
                      reason: "request",
                      operation: "append",
                      message: "offline",
                    }),
                  )
                : writer.append(id, options),
          }),
          Effect.result,
        );
        expect(failed._tag).toBe("Failure");
        expect(yield* readAll(a)).toEqual([1]);
        expect(yield* readAll(b)).toEqual([]);
        expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.none());
        const pinned = yield* (yield* Checkpoints).load(projection);
        expect(Option.getOrThrow(pinned.record).pending?.seqs).toEqual({ a: 0, b: 0 });
        yield* Streams.append(input, [2]);
        const outcomes: string[] = [];
        yield* Projection.run(projection).pipe(
          Effect.provideService(StreamsWriter, {
            ...writer,
            append: (id, options) =>
              writer.append(id, options).pipe(
                Effect.tap((result) =>
                  Effect.sync(() => {
                    outcomes.push(result._tag);
                  }),
                ),
              ),
          }),
        );
        expect(outcomes).toEqual(["Duplicate", "Appended", "Appended", "Appended"]);
        expect(states).toEqual([0, 0, 1]);
        expect(yield* readAll(a)).toEqual([1, 3]);
        expect(yield* readAll(b)).toEqual([1, 3]);
        expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(3));
        // Fail another unit after state exists: its pin must retain that prior value.
        yield* Streams.append(input, [4]);
        yield* Projection.run(projection).pipe(
          Effect.provideService(StreamsWriter, {
            ...writer,
            append: (id, options) =>
              id === b.id
                ? Effect.fail(
                    new TransportFault({
                      reason: "request",
                      operation: "append",
                      message: "offline again",
                    }),
                  )
                : writer.append(id, options),
          }),
          Effect.result,
        );
        expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(3));
        yield* Projection.run(projection);
        expect(states).toEqual([0, 0, 1, 3, 3]);
        expect(yield* readAll(a)).toEqual([1, 3, 7]);
        expect(yield* readAll(b)).toEqual([1, 3, 7]);
        expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(7));
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: failed settle rolls back checkpoint and state together`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const projection = Projection.outputs({
          id: "atomic-state",
          inputs: { input },
          outputs: { total: Output.value(Schema.Finite) },
          process: Projection.fold(0, (state, batch) => ({ state: state + batch.input.items[0]! })),
        });
        const state = yield* Projection.State;
        const failed = yield* Projection.run(projection).pipe(
          Effect.provideService(Projection.State, {
            ...state,
            save: (key, encoded) =>
              state.save(key, encoded).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ProjectionFault({
                      phase: "checkpoint",
                      reason: "storage-failure",
                      message: "after-state",
                    }),
                  ),
                ),
              ),
          }),
          Effect.result,
        );
        expect(failed._tag).toBe("Failure");
        expect((yield* (yield* Checkpoints).load(projection)).token).toBe("0");
        expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.none());
        yield* Projection.run(projection);
        expect(yield* Projection.loadState(projection, Schema.Finite)).toEqual(Option.some(1));
      }).pipe(Effect.provide(host)),
    ));

  test(`${name}: rows encode the declared collection and key, including key-only deletes`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const RowCard = Schema.Struct({ issueId: Schema.String, title: Schema.String });
        const board = Output.rows(RowCard, { key: "issueId", stream: "board" });
        const projection = Projection.outputs({
          id: "rows",
          inputs: { input },
          outputs: { board },
          process: () =>
            Effect.succeed({
              board: [Output.upsert({ issueId: "one", title: "First" }), Output.remove("old")],
            }),
        });
        yield* Projection.run(projection);
        expect(yield* readAll(StreamRef.json("board", { schema: Schema.Unknown }))).toEqual([
          {
            type: "board",
            key: "one",
            value: { issueId: "one", title: "First" },
            headers: { operation: "upsert" },
          },
          { type: "board", key: "old", headers: { operation: "delete" } },
        ]);
      }).pipe(Effect.provide(host)),
    ));
}

test("duplicate streams, multiple values, and reserved state name are rejected at construction", () => {
  expect(() =>
    Projection.outputs({
      id: "bad",
      inputs: { input },
      outputs: { a: declarations.a, b: declarations.a },
      process: () => Effect.succeed({ a: [], b: [] }),
    }),
  ).toThrow("same stream");
  expect(() =>
    Projection.outputs({
      id: "bad",
      inputs: { input },
      outputs: { a: Output.value(Schema.Finite), b: Output.value(Schema.Finite) },
      process: () => Effect.succeed({ state: 0 }),
    }),
  ).toThrow("one value");
  expect(() =>
    Projection.outputs({
      id: "bad",
      inputs: { input },
      outputs: { state: declarations.a },
      process: () => Effect.succeed({ state: [] }),
    }),
  ).toThrow("reserved");
});

// Compile-only public result constraints. Each invalid handler must be rejected.
const checkTypes = () => {
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error missing b
    process: () => Effect.succeed({ a: [1] }),
  });
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error undeclared c
    process: () => Effect.succeed({ a: [1], b: [], c: [] }),
  });
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error wrong a item type
    process: () => Effect.succeed({ a: ["bad"], b: [] }),
  });
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error each may return only declared output keys
    process: Projection.each(() => Effect.succeed({ a: [1], b: [], wrong: [] })),
  });
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: { ...declarations, total: Output.value(Schema.Finite) },
    // @ts-expect-error fold output keys are checked too
    process: Projection.fold(0, (state) => ({ state, a: [], b: [], wrong: [] })),
  });
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: { total: Output.value(Schema.Finite) },
    // @ts-expect-error raw handlers must match the declared value schema
    process: () => Effect.succeed({ state: "bad" }),
  });
  Projection.outputs({
    id: "type",
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error each cannot return a state key
    process: Projection.each(() => Effect.succeed({ a: [], b: [], state: [] })),
  });
};
void checkTypes;
