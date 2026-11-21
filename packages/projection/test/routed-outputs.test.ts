/* oxlint-disable effecttsgo/strict-effect-provide -- Each test entry point provisions its own isolated SQLite host. */
import { expect, test } from "vitest";
import { it } from "@effect/vitest";
import { Array, Effect, Option, Result, Schema } from "effect";
import { Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection, ProjectionFault } from "@streamsy/projection";
import { withFamilies } from "./support/families.ts";
import { layerSqlite } from "./support/sqlite.ts";

class Source extends Streamsy.json<Source>()("Routed/Source", "routed/source/:workspace", {
  params: { workspace: Schema.String },
  schema: Schema.Struct({ date: Schema.String, n: Schema.Finite }),
}) {}

class Output extends Streamsy.json<Output>()(
  "Routed/Output",
  "routed/out/:workspace/:date/g/:generation",
  {
    params: { workspace: Schema.String, date: Schema.String, generation: Schema.FiniteFromString },
    schema: Schema.Finite,
  },
) {}

const params = { workspace: "w" };

const at = (date: string, generation = 1) => ({ ...params, date, generation });

const outputs = { out: Projection.perGeneration(Output, "generation", { routeBy: ["date"] }) };

const definition = {
  id: "routed",
  params: { workspace: Schema.String },
  inputs: { source: Source },
  outputs,
  commit: Commit.transaction,
};

const declaration = (generation = 1) =>
  Projection.process({
    ...definition,
    generation,
    process: (batch, _unit, { outputs: { out } }) =>
      Effect.gen(function* () {
        const { wire } = yield* Output;

        for (const { date, n } of batch.source.items) {
          const address = yield* out.open({ date });
          yield* Streams.append(address, [n]).pipe(Effect.provideService(StreamsWriter, wire));
        }
      }),
  });

const seed = (items: Array.NonEmptyReadonlyArray<{ readonly date: string; readonly n: number }>) =>
  Effect.gen(function* () {
    const source = yield* Source;
    yield* source.create(params);
    yield* source.append(params, items);
  });

const retiredRoutes = (result: "closed" | "removed") => ({
  out: Array.map(["one", "two"], (date) => ({ id: Output.at(at(date)).id, result })),
});

const host = withFamilies(layerSqlite(), Source, Output);

it.live("opens two routes lazily, reuses them, and preserves older generations", () =>
  Effect.gen(function* () {
    const binding = declaration().at(params);
    const newer = declaration(2).at(params);
    const target = yield* Output;
    const store = yield* CheckpointStore;
    const routes = store.routes!;

    yield* Projection.createOutputs(binding);
    expect(binding.streams).toEqual({});
    expect(yield* target.head(at("one")).pipe(Effect.result)).toMatchObject({
      failure: { _tag: "StreamNotFound" },
    });
    yield* target.create(at("one"));
    yield* seed([
      { date: "one", n: 1 },
      { date: "two", n: 2 },
      { date: "two", n: 3 },
    ]);

    yield* Projection.run(binding);
    expect(yield* routes.list(binding)).toEqual([
      { output: "out", streamId: Output.at(at("one")).id },
      { output: "out", streamId: Output.at(at("two")).id },
    ]);
    expect(yield* store.list!("routed")).toEqual([{ generation: 1, params }]);
    expect((yield* target.collect(at("one"))).items).toEqual([1]);
    expect((yield* target.collect(at("two"))).items).toEqual([2, 3]);

    yield* seed([{ date: "one", n: 4 }]);

    const noCreate = {
      ...target,
      wire: { ...target.wire, create: () => Effect.die("Opened route was created again") },
    };

    yield* Projection.run(binding).pipe(Effect.provideService(Output, noCreate));
    const before = yield* target.wire.read(Output.at(at("one")).id, { offset: ZERO_OFFSET });
    yield* Projection.run(newer);
    expect((yield* target.collect(at("one", 2))).items).toEqual([1, 4]);
    expect(yield* target.wire.read(Output.at(at("one")).id, { offset: ZERO_OFFSET })).toEqual(
      before,
    );
    expect(yield* routes.list(binding)).toHaveLength(2);
    expect(yield* routes.list(newer)).toHaveLength(2);
  }).pipe(Effect.provide(host)),
);

it.live("rolls back a new route, its append and checkpoint when another route is populated", () =>
  Effect.gen(function* () {
    const binding = declaration().at(params);
    const target = yield* Output;
    const store = yield* CheckpointStore;
    yield* target.create(at("populated"));
    yield* target.append(at("populated"), [9]);
    yield* seed([
      { date: "new", n: 1 },
      { date: "populated", n: 2 },
    ]);

    expect(yield* Projection.run(binding).pipe(Effect.result)).toMatchObject({
      failure: {
        reason: "invalid-output",
        message: expect.stringContaining("must start empty and open"),
      },
    });
    expect((yield* store.load(binding)).record).toEqual(Option.none());
    expect(yield* store.routes!.list(binding)).toEqual([]);
    expect(yield* target.head(at("new")).pipe(Effect.result)).toMatchObject({
      failure: { _tag: "StreamNotFound" },
    });
    expect((yield* target.collect(at("populated"))).items).toEqual([9]);
  }).pipe(Effect.provide(host)),
);

it.live("keeps closed routes for a later removal and makes retirement repeatable", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const store = yield* CheckpointStore;
    yield* seed([
      { date: "one", n: 1 },
      { date: "two", n: 2 },
    ]);
    yield* Projection.run(binding);

    const retire = (streams: "close" | "remove") =>
      Projection.retire(spec, params, { generation: 1, streams });

    expect(yield* retire("close")).toEqual({
      record: "retired",
      outputs: {},
      routed: retiredRoutes("closed"),
    });
    expect(yield* store.routes!.list(binding)).toHaveLength(2);
    yield* Projection.forget(binding);
    expect(yield* retire("remove")).toEqual({
      record: "already-retired",
      outputs: {},
      routed: retiredRoutes("removed"),
    });
    expect(yield* store.routes!.list(binding)).toEqual([]);
    expect(yield* retire("remove")).toEqual({
      record: "already-retired",
      outputs: {},
      routed: { out: [] },
    });
    expect(yield* store.load(binding).pipe(Effect.result)).toMatchObject({
      failure: { reason: "retired-generation" },
    });
  }).pipe(Effect.provide(host)),
);

it.live("forget clears route ownership and a replay still requires empty streams", () =>
  Effect.gen(function* () {
    const binding = declaration().at(params);
    const store = yield* CheckpointStore;
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    yield* Projection.forget(binding);
    expect(yield* store.routes!.list(binding)).toEqual([]);
    expect((yield* store.load(binding)).record).toEqual(Option.none());
    expect(yield* Projection.run(binding).pipe(Effect.result)).toMatchObject({
      failure: { reason: "invalid-output" },
    });
  }).pipe(Effect.provide(host)),
);

it.live("requires a process transaction and shares the checkpoint owner", () =>
  Effect.gen(function* () {
    const target = yield* Output;
    yield* seed([{ date: "one", n: 1 }]);

    const binding = Projection.process({
      ...definition,
      prepare: (_batch, _unit, context) => context.outputs.out.open({ date: "one" }),
      process: () => Effect.void,
    }).at(params);

    expect(yield* Projection.run(binding).pipe(Effect.result)).toMatchObject({
      failure: {
        reason: "unsupported-composition",
        message: expect.stringContaining("process transaction"),
      },
    });
    expect(
      yield* Projection.run(declaration().at(params)).pipe(
        Effect.provideService(Output, { ...target, owner: {} }),
        Effect.result,
      ),
    ).toMatchObject({
      failure: {
        reason: "unsupported-composition",
        message: expect.stringContaining("transaction owner"),
      },
    });
  }).pipe(Effect.provide(host)),
);

it.live("fails retirement without listing support and rolls back its tombstone", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const store = yield* CheckpointStore;
    yield* seed([{ date: "one", n: 1 }]);

    const unsupported = new ProjectionFault({
      phase: "load",
      reason: "unsupported-composition",
      message: "Owner cannot list records",
    });

    const limited = {
      ...store,
      routes: { ...store.routes!, list: () => Effect.fail(unsupported) },
    };

    yield* Projection.run(binding).pipe(Effect.provideService(CheckpointStore, limited));
    const before = yield* store.load(binding);

    const failed = yield* Projection.retire(spec, params, { generation: 1 }).pipe(
      Effect.provideService(CheckpointStore, limited),
      Effect.result,
    );

    expect(failed).toEqual(Result.fail(unsupported));
    expect(yield* store.load(binding)).toEqual(before);
    expect((yield* (yield* Output).collect(at("one"))).items).toEqual([1]);
  }).pipe(Effect.provide(host)),
);

const process = () => Effect.void;

test("rejects unsupported routed declarations at runtime", () => {
  expect(() =>
    Projection.process({
      ...definition,
      params: { ...definition.params, date: Schema.String },
      // @ts-expect-error A routed parameter cannot come from the binding.
      outputs,
      process,
    }),
  ).toThrow(/must not be a generation or projection parameter/);

  expect(() =>
    Projection.process({
      ...definition,
      outputs: {
        // @ts-expect-error A shared generation cannot be routed.
        out: Projection.perGeneration(Output, "generation", { routeBy: ["date"], shared: true }),
      },
      process,
    }),
  ).toThrow(/cannot be shared/);

  expect(() =>
    Projection.process({
      ...definition,
      // @ts-expect-error Only declared route names are accepted.
      outputs: { out: Projection.perGeneration(Output, "generation", { routeBy: ["missing"] }) },
      process,
    }),
  ).toThrow(/must be a route parameter/);

  const producer = { ...definition, commit: Commit.producer, process };
  // @ts-expect-error A producer does not supply the owner transaction.
  expect(() => Projection.process(producer)).toThrow(/require Commit.transaction/);

  const emitter = { ...definition, emit: () => ({}) };
  // @ts-expect-error Routed output requires a process handler.
  expect(() => Projection.emit(emitter)).toThrow(/require Projection.process/);
});
