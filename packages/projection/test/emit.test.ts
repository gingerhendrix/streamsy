import { DurableState } from "@streamsy/core";
import { Commit, Transaction } from "@streamsy/projection";
import * as FamilyFixture from "./support/families.ts";
/* oxlint-disable effecttsgo/strict-effect-provide -- Tests own the runtime boundary and complete host graph. */
import { expect, test } from "vitest";
import { Effect, Layer, Option, Schema } from "effect";
import { TransportFault } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";
import { BunStorage } from "@streamsy/storage/bun";
import { readAll } from "./support/scenarios.ts";

const hosts: ReadonlyArray<
  readonly [
    string,
    Layer.Layer<
      | import("@streamsy/projection").CheckpointStore
      | import("@streamsy/core/protocol").StreamsReader
      | import("@streamsy/core/protocol").StreamsWriter,
      import("@streamsy/core").StorageFault | ProjectionFault
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
] as const;

const input = FamilyFixture.json("named-input", { schema: Schema.Finite });

const a = FamilyFixture.json("named-a", { schema: Schema.Finite });

const b = FamilyFixture.json("named-b", { schema: Schema.Finite });

const declarations = {
  a: a,
  b: b,
};

const seed = Streams.create(input).pipe(Effect.andThen(Streams.append(input, [1])));

const numbers = FamilyFixture.json("numbers", { schema: Schema.Finite });

const Card = Schema.Struct({ issueId: Schema.String, total: Schema.Finite });

const tracker = Projection.mapAccumBatch({
  commit: Commit.producer,
  id: "tracker",
  params: {},
  inputs: { numbers },
  outputs: {
    board: FamilyFixture.state("board", {
      collections: { board: { schema: Card, key: "issueId" } },
    }),
    transitions: FamilyFixture.json("transitions", { schema: Schema.Finite }),
  },
  state: { schema: Schema.Finite, initial: 0 },
  mapAccumBatch: (state, batch) =>
    Effect.sync(() => {
      const next = state + batch.numbers.items.reduce((sum, n) => sum + n, 0);

      return [
        next,
        {
          board: [
            DurableState.upsert("board", { issueId: "total", total: next }),
            DurableState.remove("board", "old"),
          ],
          transitions: [next],
        },
      ] as const;
    }),
}).at({});

for (const [name, host] of hosts) {
  test(`${name}: replay rejects items for an output that was empty at pin`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        yield* Streams.create(b);
        let replay = false;

        const projection = Projection.emit({
          commit: Commit.producer,
          id: "changed-replay",
          params: {},
          inputs: { input },
          outputs: declarations,
          emit: () => Effect.succeed({ a: [1], b: replay ? [1] : [] }),
        }).at({});

        const writer = yield* StreamsWriter;

        const failed = yield* Projection.run(projection).pipe(
          FamilyFixture.provideWriter({
            ...writer,
            append: (id, options) =>
              writer.append(id, options).pipe(
                Effect.andThen(
                  Effect.fail(
                    new TransportFault({
                      reason: "response",
                      operation: "append",
                      message: "lost reply",
                    }),
                  ),
                ),
              ),
          }),
          Effect.result,
        );

        expect(failed._tag).toBe("Failure");
        const owner = yield* CheckpointStore;
        const before = yield* owner.load(projection);
        expect(Option.getOrThrow(before.record).pending?.seqs).toEqual({ a: 0 });
        replay = true;
        const result = yield* Projection.run(projection).pipe(Effect.result);
        expect(result._tag).toBe("Failure");

        if (result._tag === "Failure")
          expect(result.failure).toMatchObject({
            phase: "process",
            reason: "invalid-output",
            message: "Cannot reproduce pinned unit: output b has items that were not pinned",
          });
        expect(yield* owner.load(projection)).toEqual(before);
        expect(yield* readAll(a)).toEqual([1]);
        expect(yield* readAll(b)).toEqual([]);
      }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
    ));
  test(`${name}: mapAccumBatch writes rows, events and state`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* Streams.create(numbers);
        yield* Streams.append(numbers, [1, 2, 3]);
        yield* Projection.run(tracker);
        expect((yield* Projection.read(tracker)).state).toBe(6);
        expect(
          yield* readAll(FamilyFixture.json("transitions", { schema: Schema.Finite })),
        ).toEqual([6]);
        expect(yield* readAll(FamilyFixture.json("board", { schema: Schema.Unknown }))).toEqual([
          {
            type: "board",
            key: "total",
            value: { issueId: "total", total: 6 },
            headers: { operation: "upsert" },
          },
          { type: "board", key: "old", headers: { operation: "delete" } },
        ]);
      }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
    ));
  test(`${name}: independent sequences skip empty outputs and versions own their producer ids`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;

        const projection = Projection.emit({
          commit: Commit.producer,
          id: "named",
          params: {},
          inputs: { input },
          outputs: declarations,
          emit: ({ item }) => Effect.succeed({ a: [item * 10], b: item % 2 === 0 ? [item] : [] }),
        }).at({});

        const saved: import("@streamsy/projection/checkpoint").CheckpointRecord[] = [];
        const owner = yield* CheckpointStore;
        yield* Projection.run(projection).pipe(
          Effect.provideService(CheckpointStore, {
            ...owner,
            compareAndSet: (key, token, record) => {
              saved.push(record);

              return owner.compareAndSet(key, token, record);
            },
          }),
        );
        // An owner may retain the passed record: settling must not mutate that pin.
        expect(saved[0]?.adapters.outputs?.a?.nextSeq).toBe(0);
        expect(saved[1]?.adapters.outputs?.a?.nextSeq).toBe(1);
        expect(yield* (yield* CheckpointStore).load(projection)).toMatchObject({
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

        const v2 = Projection.emit({
          commit: Commit.producer,
          id: "named",
          params: {},
          version: 2,
          inputs: { input },
          outputs: declarations,
          emit: (entry) => Effect.succeed({ a: [entry.item * 100], b: [] }),
        }).at({});

        yield* Projection.run(v2).pipe(
          FamilyFixture.provideWriter({
            ...writer,
            append: (id, options) => {
              ids.push(options.producer?.producerId ?? "");

              return writer.append(id, options);
            },
          }),
        );
        expect(ids).toEqual(["named/v2"]);
        expect(yield* readAll(a)).toEqual([10, 20, 100, 200]);
      }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
    ));

  test(`${name}: a stop between appends replays from pre-unit state and settles once`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        const states: number[] = [];

        const projection = Projection.mapAccumBatch({
          commit: Commit.producer,
          id: "recover",
          params: {},
          inputs: { input },
          outputs: declarations,
          state: { schema: Schema.Finite, initial: 0 },
          mapAccumBatch: (state, batch) =>
            Effect.sync(() => {
              states.push(state);
              const next = state + batch.input.items.reduce((sum, n) => sum + n, 0);

              return [next, { a: [next], b: [next] }] as const;
            }),
        }).at({});

        const writer = yield* StreamsWriter;

        const failed = yield* Projection.run(projection).pipe(
          FamilyFixture.provideWriter({
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
        expect(
          Option.flatMap((yield* (yield* CheckpointStore).load(projection)).record, (record) =>
            Option.fromUndefinedOr(record.state),
          ),
        ).toEqual(Option.none());
        const pinned = yield* (yield* CheckpointStore).load(projection);
        expect(Option.getOrThrow(pinned.record).pending?.seqs).toEqual({ a: 0, b: 0 });
        yield* Streams.append(input, [2]);
        const outcomes: string[] = [];
        yield* Projection.run(projection).pipe(
          FamilyFixture.provideWriter({
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
        expect((yield* Projection.read(projection)).state).toBe(3);
        // Fail another unit after state exists: its pin must retain that prior value.
        yield* Streams.append(input, [4]);
        yield* Projection.run(projection).pipe(
          FamilyFixture.provideWriter({
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
        expect((yield* Projection.read(projection)).state).toBe(3);
        yield* Projection.run(projection);
        expect(states).toEqual([0, 0, 1, 3, 3]);
        expect(yield* readAll(a)).toEqual([1, 3, 7]);
        expect(yield* readAll(b)).toEqual([1, 3, 7]);
        expect((yield* Projection.read(projection)).state).toBe(7);
      }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
    ));

  test(`${name}: failed settle rolls back checkpoint and state together`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;

        const projection = Projection.mapAccumBatch({
          commit: Commit.producer,
          id: "transactional-state",
          params: {},
          inputs: { input },
          outputs: {},
          state: { schema: Schema.Finite, initial: 0 },
          mapAccumBatch: (state, batch) =>
            Effect.succeed([state + batch.input.items[0]!, {}] as const),
        }).at({});

        const store = yield* CheckpointStore;

        const failed = yield* Projection.run(projection).pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: () =>
              Effect.fail(
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "storage-failure",
                  message: "before-settle",
                }),
              ),
          }),
          Effect.result,
        );

        expect(failed._tag).toBe("Failure");

        if (failed._tag === "Failure")
          expect(failed.failure).toMatchObject({ phase: "checkpoint", reason: "storage-failure" });
        expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("0");
        expect(
          Option.flatMap((yield* (yield* CheckpointStore).load(projection)).record, (record) =>
            Option.fromUndefinedOr(record.state),
          ),
        ).toEqual(Option.none());
        yield* Projection.run(projection);
        expect((yield* Projection.read(projection)).state).toBe(1);
      }).pipe(Effect.provide(FamilyFixture.withFamilies(host))),
    ));
}

test("state is an ordinary output name; duplicate streams fail at at", () => {
  expect(() =>
    Projection.emit({
      commit: Commit.producer,
      id: "bad",
      params: {},
      inputs: { input },
      outputs: { a: declarations.a, b: declarations.a },
      emit: () => Effect.succeed({ a: [], b: [] }),
    }).at({}),
  ).toThrow("same stream");
  expect(() =>
    Projection.emit({
      commit: Commit.producer,
      id: "bad",
      params: {},
      inputs: { input },
      outputs: { state: declarations.a },
      emit: () => Effect.succeed({ state: [] }),
    }),
  ).not.toThrow();
});

// Compile-only public result constraints. Each invalid handler must be rejected.
const checkTypes = () => {
  Projection.emit({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: declarations,
    emit: () => Effect.succeed({ a: [1] }),
  }).at({});
  Projection.emit({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error undeclared c
    emit: () => Effect.succeed({ a: [1], b: [], c: [] }),
  }).at({});
  Projection.emit({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error wrong a item type
    emit: () => Effect.succeed({ a: ["bad"], b: [] }),
  }).at({});
  Projection.emit({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error each may return only declared output keys
    emit: () => Effect.succeed({ a: [1], b: [], wrong: [] }),
  }).at({});
  Projection.mapAccumBatch({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: declarations,
    state: { schema: Schema.Finite, initial: 0 },
    // @ts-expect-error output keys are checked with state too
    mapAccumBatch: (state) => Effect.succeed([state, { a: [], b: [], wrong: [] }] as const),
  }).at({});
  Projection.mapAccumBatch({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: {},
    state: { schema: Schema.Finite, initial: 0 },
    // @ts-expect-error the returned state must match the declared state schema
    mapAccumBatch: () => Effect.succeed(["bad", {}] as const),
  }).at({});
  Projection.emit({
    commit: Commit.producer,
    id: "type",
    params: {},
    inputs: { input },
    outputs: declarations,
    // @ts-expect-error each cannot return a state key
    emit: () => Effect.succeed({ a: [], b: [], state: [] }),
  }).at({});
};

void checkTypes;

test("named outputs reject an output that names one of their inputs", () => {
  for (const declaration of [
    input,
    FamilyFixture.state(input.id, { collections: { board: { schema: Card, key: "issueId" } } }),
  ]) {
    expect(() =>
      Projection.emit({
        commit: Commit.producer,
        id: "feedback",
        params: {},
        inputs: { input },
        outputs: { feedback: declaration },
        emit: () => Effect.succeed({ feedback: [] }),
      }).at({}),
    ).toThrow("An output may not name an input stream");
  }
});
