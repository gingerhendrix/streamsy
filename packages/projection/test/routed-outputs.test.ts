/* oxlint-disable effecttsgo/strict-effect-provide -- Each test entry point provisions its own isolated SQLite host. */
import { expect, test } from "vitest";
import { it } from "@effect/vitest";
import { Array, Effect, Layer, Option, Result, Schedule, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { TestClock } from "effect/testing";
import { StorageFault, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { OWNER_RECORDS_LIST_UNSUPPORTED, OwnerRecords } from "@streamsy/core/internal";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Commit, Projection, ProjectionFault } from "@streamsy/projection";
import { fromStore, recordKey } from "@streamsy/projection/checkpoint";
import { eventually } from "./support/eventually.ts";
import { withFamilies } from "./support/families.ts";
import { Accepted, Numbers, Rows, accepted, output } from "./support/generation-outputs.ts";
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

const declaration = (generation = 1, commit = Commit.transaction) =>
  Projection.process({
    ...definition,
    commit,
    generation,
    retry: { schedule: Schedule.spaced("1 second") },
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

const retiredRoutes = (result: "closed" | "removed") =>
  Array.map(["one", "two"], (date) => ({ output: "out", id: Output.at(at(date)).id, result }));

const host = withFamilies(layerSqlite(), Source, Output);

it.live("absent routed forget skips removal when owner-record listing is unsupported", () =>
  Effect.gen(function* () {
    const owner = yield* OwnerRecords;

    const fault = new StorageFault({
      operation: OWNER_RECORDS_LIST_UNSUPPORTED,
      message: "Owner-record listing is unsupported",
      retryable: false,
    });

    const noListing = Layer.succeed(OwnerRecords, { ...owner, list: () => Effect.fail(fault) });

    const store = yield* CheckpointStore.pipe(
      Effect.provide(CheckpointStore.layer.pipe(Layer.fresh, Layer.provide(noListing))),
    );

    const absentStore = { ...store, remove: () => Effect.die("Unexpected checkpoint removal") };
    let cleaned = 0;
    const cleanup = Commit.transactionWithCleanup(() => Effect.sync(() => void (cleaned += 1)));

    yield* Projection.forget(declaration().at(params)).pipe(
      Effect.provideService(CheckpointStore, absentStore),
    );
    yield* Projection.forget(declaration(1, cleanup).at(params)).pipe(
      Effect.provideService(CheckpointStore, absentStore),
    );
    expect(cleaned).toBe(1);

    const binding = declaration().at(params);
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    const source = yield* Source;
    yield* source.wire.remove(Source.at(params).id);
    expect(
      yield* Projection.run(binding).pipe(
        Effect.provideService(CheckpointStore, store),
        Effect.flip,
      ),
    ).toMatchObject({ reason: "unsupported-composition" });
    expect(Option.getOrThrow((yield* store.load(binding)).record).ended).toBe(true);
    expect(yield* store.routes!.state(binding, "out", Output.at(at("one")).id)).toBe("live");
  }).pipe(Effect.provide(host)),
);

it.live(
  "absent routed forget propagates owner-record listing failures other than unsupported",
  () =>
    Effect.gen(function* () {
      const owner = yield* OwnerRecords;

      const fault = new StorageFault({
        operation: "records.list",
        message: "Listing failed",
        retryable: true,
      });

      const failedListing = Layer.succeed(OwnerRecords, {
        ...owner,
        list: () => Effect.fail(fault),
      });

      expect(
        yield* Projection.forget(declaration().at(params)).pipe(
          Effect.provide(CheckpointStore.layer.pipe(Layer.fresh, Layer.provide(failedListing))),
          Effect.flip,
        ),
      ).toMatchObject({ reason: "storage-failure", cause: fault });
    }).pipe(Effect.provide(host)),
);

it.live("the absent-checkpoint listing exception does not hide a route write failure", () =>
  Effect.gen(function* () {
    const unsupported = new ProjectionFault({
      phase: "checkpoint",
      reason: "unsupported-composition",
      message: "The adapter cannot write route records",
    });

    const { routes } = fromStore({
      list: () => Effect.succeed([["route", "{}"]]),
      read: () => Effect.succeedNone,
      write: () => Effect.fail(unsupported),
      remove: () => Effect.void,
      withTransaction: (effect) => effect,
    });

    expect(
      yield* routes!
        .forget(declaration().at(params), { ignoreUnsupportedList: true })
        .pipe(Effect.flip),
    ).toBe(unsupported);
  }),
);

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

    const noStreamRead = {
      ...target,
      wire: {
        ...target.wire,
        create: () => Effect.die("Opened route was created again"),
        head: () => Effect.die("Opened route was checked again"),
      },
    };

    yield* Projection.run(binding).pipe(Effect.provideService(Output, noStreamRead));
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
      streams: retiredRoutes("closed"),
    });
    expect(yield* store.routes!.list(binding)).toHaveLength(2);
    yield* Projection.forget(binding);
    expect(yield* store.routes!.state(binding, "out", Output.at(at("one")).id)).toBe("live");
    expect(yield* retire("remove")).toEqual({
      record: "already-retired",
      streams: retiredRoutes("removed"),
    });
    expect(yield* store.routes!.list(binding)).toEqual([]);
    expect(yield* retire("remove")).toEqual({
      record: "already-retired",
      streams: [],
    });
    expect(yield* store.load(binding).pipe(Effect.result)).toMatchObject({
      failure: { reason: "retired-generation" },
    });
  }).pipe(Effect.provide(host)),
);

it.live("case A: explicit forget preserves all routes for retirement", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const target = yield* Output;
    yield* seed([
      { date: "one", n: 1 },
      { date: "two", n: 2 },
    ]);
    yield* Projection.run(binding);
    yield* Projection.forget(binding);

    expect(yield* Projection.retire(spec, params, { generation: 1 })).toEqual({
      record: "retired",
      streams: retiredRoutes("removed"),
    });

    for (const date of ["one", "two"]) {
      expect(yield* target.head(at(date)).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
    }
  }).pipe(Effect.provide(host)),
);

it.live("case B: a gone input preserves all routes for retirement", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const source = yield* Source;
    const target = yield* Output;
    const store = yield* CheckpointStore;
    yield* seed([
      { date: "one", n: 1 },
      { date: "two", n: 2 },
    ]);
    yield* Projection.run(binding);
    yield* source.wire.remove(Source.at(params).id);
    expect((yield* Projection.run(binding)).status).toBe("source-closed");
    expect((yield* store.load(binding)).token).toBe("0");

    expect(yield* Projection.retire(spec, params, { generation: 1 })).toEqual({
      record: "retired",
      streams: retiredRoutes("removed"),
    });

    for (const date of ["one", "two"]) {
      expect(yield* target.head(at(date)).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
    }
  }).pipe(Effect.provide(host)),
);

it.live("replay after forget reclaims a route only if its stream starts empty and open", () =>
  Effect.gen(function* () {
    const binding = declaration().at(params);
    const store = yield* CheckpointStore;
    const records = yield* OwnerRecords;
    const routes = store.routes!;
    const target = yield* Output;
    const id = Output.at(at("one")).id;
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    yield* Projection.forget(binding);
    expect(yield* routes.has(binding, "out", id)).toBe(true);
    expect(yield* routes.state(binding, "out", id)).toBe("forgotten");
    expect(yield* records.list('["streamsy.projection.routed.v1",')).toEqual([
      [expect.any(String), '{"forgotten":true}'],
    ]);
    expect(yield* Projection.run(binding).pipe(Effect.result)).toMatchObject({
      failure: { reason: "invalid-output" },
    });
    expect((yield* store.load(binding)).token).toBe("0");
    expect(yield* routes.state(binding, "out", id)).toBe("forgotten");
    expect((yield* target.collect(at("one"))).items).toEqual([1]);

    yield* target.wire.remove(id);
    yield* Projection.run(binding);
    expect(yield* routes.state(binding, "out", id)).toBe("live");
    expect((yield* target.collect(at("one"))).items).toEqual([1]);
  }).pipe(Effect.provide(host)),
);

it.live("after forget, reopening one route leaves the other forgotten", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const target = yield* Output;
    const store = yield* CheckpointStore;
    const routes = store.routes!;
    const one = Output.at(at("one")).id;
    const two = Output.at(at("two")).id;
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    yield* seed([{ date: "two", n: 2 }]);
    yield* Projection.run(binding);
    yield* Projection.forget(binding);
    yield* target.wire.remove(one);

    yield* Projection.run(binding, { limit: 1 });
    expect(yield* routes.state(binding, "out", one)).toBe("live");
    expect(yield* routes.state(binding, "out", two)).toBe("forgotten");
    const before = yield* store.load(binding);
    expect(yield* Projection.run(binding).pipe(Effect.flip)).toMatchObject({
      reason: "invalid-output",
    });
    expect(yield* store.load(binding)).toEqual(before);
    expect((yield* target.collect(at("two"))).items).toEqual([2]);

    yield* Projection.retire(spec, params, { generation: 1, streams: "close" });
    expect(yield* routes.state(binding, "out", one)).toBe("live");
    expect(yield* routes.state(binding, "out", two)).toBe("forgotten");
    expect(yield* Projection.retire(spec, params, { generation: 1 })).toMatchObject({
      record: "already-retired",
      streams: retiredRoutes("removed"),
    });
  }).pipe(Effect.provide(withFamilies(layerSqlite(1), Source, Output))),
);

it.live("a failed unit rolls back a successful reclaim of a forgotten route", () =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;
    const routes = store.routes!;
    const target = yield* Output;
    const id = Output.at(at("one")).id;
    const binding = declaration().at(params);
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    yield* Projection.forget(binding);
    yield* target.wire.remove(id);

    const failing = Projection.process({
      ...definition,
      process: (_batch, _unit, { outputs: { out } }) =>
        Effect.gen(function* () {
          yield* out.open({ date: "one" });
          expect(yield* routes.state(binding, "out", id)).toBe("live");

          return yield* Effect.fail("rollback");
        }),
    }).at(params);

    expect(yield* Projection.run(failing).pipe(Effect.flip)).toBe("rollback");
    expect(yield* routes.state(binding, "out", id)).toBe("forgotten");
    expect((yield* store.load(binding)).token).toBe("0");
    expect(yield* target.head(at("one")).pipe(Effect.flip)).toMatchObject({
      _tag: "StreamNotFound",
    });
  }).pipe(Effect.provide(host)),
);

it.live("a failing forget cleanup restores a live route and its checkpoint", () =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;
    const routes = store.routes!;
    const id = Output.at(at("one")).id;

    const failure = new ProjectionFault({
      phase: "checkpoint",
      reason: "storage-failure",
      message: "Cleanup failed",
    });

    const commit = Commit.transactionWithCleanup((key) =>
      Effect.gen(function* () {
        expect(yield* routes.state(key, "out", id)).toBe("forgotten");
        expect((yield* store.load(key)).token).toBe("0");

        return yield* failure;
      }),
    );

    const binding = declaration(1, commit).at(params);
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    const before = yield* store.load(binding);

    expect(yield* Projection.forget(binding).pipe(Effect.flip)).toBe(failure);
    expect(yield* routes.state(binding, "out", id)).toBe("live");
    expect(yield* store.load(binding)).toEqual(before);
  }).pipe(Effect.provide(host)),
);

it.live("a forgotten route still prevents another output from claiming its stream", () =>
  Effect.gen(function* () {
    const store = yield* CheckpointStore;
    const routes = store.routes!;
    const target = yield* Output;
    const id = Output.at(at("one")).id;
    const binding = declaration().at(params);
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    yield* Projection.forget(binding);
    yield* target.wire.remove(id);

    const overlap = Projection.process({
      ...definition,
      outputs: { ...outputs, alias: outputs.out },
      process: (_batch, _unit, { outputs: { alias } }) =>
        alias.open({ date: "one" }).pipe(Effect.asVoid),
    }).at(params);

    expect(yield* Projection.run(overlap).pipe(Effect.flip)).toMatchObject({
      reason: "invalid-output",
    });
    expect((yield* store.load(binding)).token).toBe("0");
    expect(yield* routes.state(binding, "out", id)).toBe("forgotten");
    expect(yield* routes.state(binding, "alias", id)).toBe("absent");
    expect(yield* target.head(at("one")).pipe(Effect.flip)).toMatchObject({
      _tag: "StreamNotFound",
    });
  }).pipe(Effect.provide(host)),
);

it.live("forget marks routes of an undecodable or absent checkpoint for retirement", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const records = yield* OwnerRecords;
    const store = yield* CheckpointStore;
    const routes = store.routes!;
    const one = Output.at(at("one")).id;
    const two = Output.at(at("two")).id;
    const slot = recordKey(binding);
    yield* seed([
      { date: "one", n: 1 },
      { date: "two", n: 2 },
    ]);
    yield* Projection.run(binding);
    yield* records.write(slot, "{}");

    yield* Projection.forget(binding);
    expect(yield* records.read(slot)).toEqual(Option.none());
    expect(yield* routes.state(binding, "out", one)).toBe("forgotten");
    expect(yield* routes.state(binding, "out", two)).toBe("forgotten");

    yield* routes.add(binding, "out", two);
    yield* Projection.forget(binding);
    expect(yield* records.read(slot)).toEqual(Option.none());
    expect(yield* routes.state(binding, "out", two)).toBe("forgotten");

    expect(yield* Projection.retire(spec, params, { generation: 1 })).toMatchObject({
      record: "retired",
      streams: retiredRoutes("removed"),
    });
    expect(yield* routes.state(binding, "out", one)).toBe("absent");
    expect(yield* routes.state(binding, "out", two)).toBe("absent");
  }).pipe(Effect.provide(host)),
);

it.effect("a missing live route rolls back and the attached handler retries StreamNotFound", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const target = yield* Output;
    const store = yield* CheckpointStore;
    const routes = store.routes!;
    const id = Output.at(at("one")).id;
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    const before = yield* store.load(binding);
    yield* target.wire.remove(id);
    yield* seed([{ date: "one", n: 2 }]);

    expect(yield* Projection.run(binding).pipe(Effect.flip)).toMatchObject({
      _tag: "StreamNotFound",
    });
    expect(yield* store.load(binding)).toEqual(before);
    expect(yield* routes.state(binding, "out", id)).toBe("live");

    yield* Layer.build(Projection.attach(spec));
    const registry = yield* Projection.Registry;
    yield* registry.schedule(binding);
    const failed = registry.failed(spec);

    const failure = Option.some({
      retryable: true,
      message: expect.stringContaining("StreamNotFound"),
    });

    expect(yield* eventually(failed, (rows) => rows[0]?.attempts === 1)).toMatchObject([
      { attempts: 1, stopped: false, failure },
    ]);
    yield* TestClock.adjust("1 second");
    expect(yield* eventually(failed, (rows) => rows[0]?.attempts === 2)).toMatchObject([
      { attempts: 2, stopped: false, failure },
    ]);
    expect(yield* store.load(binding)).toEqual(before);
    expect(yield* routes.state(binding, "out", id)).toBe("live");
  }).pipe(Effect.provide(Layer.merge(host, Projection.Registry.layer))),
);

it.live("forget and retire keep route records of a and ab separate", () =>
  Effect.gen(function* () {
    const a = Projection.process({
      ...definition,
      id: "a",
      process: (_, __, { outputs: { out } }) => out.open({ date: "one" }).pipe(Effect.asVoid),
    });

    const ab = Projection.process({
      ...definition,
      id: "ab",
      process: (_, __, { outputs: { out } }) => out.open({ date: "two" }).pipe(Effect.asVoid),
    });

    const shorter = a.at(params);
    const longer = ab.at(params);
    const { routes } = yield* CheckpointStore;
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(shorter);
    yield* Projection.run(longer);
    yield* Projection.forget(shorter);

    const streamId = Output.at(at("two")).id;
    expect(yield* routes!.state(longer, "out", streamId)).toBe("live");
    yield* Projection.retire(a, params, { generation: 1 });
    expect(yield* routes!.list(shorter)).toEqual([]);
    expect(yield* routes!.list(longer)).toEqual([{ output: "out", streamId }]);
    expect((yield* (yield* Output).head(at("two"))).nextOffset).toBe(ZERO_OFFSET);
  }).pipe(Effect.provide(host)),
);

it.live("a malformed route value fails open and leaves the stream discoverable for retire", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const records = yield* OwnerRecords;
    const store = yield* CheckpointStore;
    const target = yield* Output;
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    const entries = yield* records.list('["streamsy.projection.routed.v1",');
    const [slot] = entries[0]!;
    yield* records.write(slot, '{"forgotten":false}');
    yield* seed([{ date: "one", n: 2 }]);
    const before = yield* store.load(binding);
    expect(yield* Projection.run(binding).pipe(Effect.flip)).toMatchObject({
      reason: "invalid-record",
    });
    expect(yield* store.load(binding)).toEqual(before);
    expect((yield* target.collect(at("one"))).items).toEqual([1]);
    expect(yield* Projection.retire(spec, params, { generation: 1 })).toMatchObject({
      streams: [{ output: "out", id: Output.at(at("one")).id, result: "removed" }],
    });
    expect(yield* target.head(at("one")).pipe(Effect.flip)).toMatchObject({
      _tag: "StreamNotFound",
    });
  }).pipe(Effect.provide(host)),
);

it.live("retires an undecodable checkpoint and removes its routed streams directly", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const records = yield* OwnerRecords;
    const target = yield* Output;
    const store = yield* CheckpointStore;
    const slot = recordKey(binding);
    yield* seed([
      { date: "one", n: 1 },
      { date: "two", n: 2 },
    ]);
    yield* Projection.run(binding);
    yield* records.write(slot, "{}");
    expect(yield* store.load(binding).pipe(Effect.flip)).toMatchObject({
      reason: "invalid-record",
    });

    const retire = Projection.retire(spec, params, { generation: 1 });
    expect(yield* retire).toMatchObject({
      record: "retired",
      streams: retiredRoutes("removed"),
    });
    expect(yield* records.read(slot)).toEqual(Option.some('{"version":1,"retired":{}}'));
    expect(yield* store.routes!.list(binding)).toEqual([]);

    for (const date of ["one", "two"]) {
      expect(yield* target.head(at(date)).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
    }

    expect((yield* retire).record).toBe("already-retired");
  }).pipe(Effect.provide(host)),
);

it.live("retire propagates a storage failure when replacing an undecodable checkpoint", () =>
  Effect.gen(function* () {
    const spec = declaration();
    const binding = spec.at(params);
    const records = yield* OwnerRecords;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* CheckpointStore;
    const target = yield* Output;
    const slot = recordKey(binding);
    yield* seed([{ date: "one", n: 1 }]);
    yield* Projection.run(binding);
    yield* records.write(slot, "{}");
    yield* sql`
      CREATE TRIGGER fail_tombstone BEFORE UPDATE ON streamsy_owner_records
      BEGIN SELECT RAISE(ABORT, 'cannot write tombstone'); END
    `;

    expect(
      yield* Projection.retire(spec, params, { generation: 1 }).pipe(Effect.flip),
    ).toMatchObject({
      reason: "storage-failure",
      message: "Cannot write a checkpoint",
    });
    expect(yield* records.read(slot)).toEqual(Option.some("{}"));
    expect(yield* store.routes!.state(binding, "out", Output.at(at("one")).id)).toBe("live");
    expect((yield* target.collect(at("one"))).items).toEqual([1]);
  }).pipe(Effect.provide(host)),
);

it.live("retire reports streams in code-unit id order within one routed output", () =>
  Effect.gen(function* () {
    const spec = declaration();
    yield* seed([
      { date: "zeta", n: 1 },
      { date: "alpha", n: 2 },
      { date: "Mid", n: 3 },
    ]);
    yield* Projection.run(spec.at(params));

    expect(yield* Projection.retire(spec, params, { generation: 1 })).toEqual({
      record: "retired",
      streams: Array.map(["Mid", "alpha", "zeta"], (date) => ({
        output: "out",
        id: Output.at(at(date)).id,
        result: "removed",
      })),
    });
  }).pipe(Effect.provide(host)),
);

it.live("sorts a mixed retirement by output and id and cleans routes in that order", () =>
  Effect.gen(function* () {
    const plainParams = { workspace: "unwrapped", rev: 1 };

    const spec = Projection.process({
      ...definition,
      outputs: {
        zRoute: outputs.out,
        shared: Projection.perGeneration(Rows, "rev", { shared: true }),
        aOwned: output,
        ARoute: outputs.out,
        plain: Numbers.at(plainParams),
      },
      accepted,
      process: (batch, _unit, { outputs: handles }) =>
        Effect.gen(function* () {
          for (const { date } of batch.source.items) {
            const handle = date === "first" ? handles.zRoute : handles.ARoute;
            yield* handle.open({ date });
          }
        }),
    });

    const nums = yield* Numbers;
    const target = yield* Output;
    yield* nums.create(plainParams);
    yield* seed([
      { date: "first", n: 1 },
      { date: "zeta", n: 2 },
      { date: "alpha", n: 3 },
      { date: "Mid", n: 4 },
    ]);
    yield* Projection.run(spec.at(params));

    const removed: string[] = [];

    const traced: typeof target = {
      ...target,
      wire: {
        ...target.wire,
        remove: (id) =>
          Effect.sync(() => {
            removed.push(id);
          }).pipe(Effect.andThen(target.wire.remove(id))),
      },
    };

    const mid = Output.at(at("Mid")).id;
    const alpha = Output.at(at("alpha")).id;
    const zeta = Output.at(at("zeta")).id;
    const first = Output.at(at("first")).id;
    expect(
      yield* Projection.retire(spec, params, { generation: 1 }).pipe(
        Effect.provideService(Output, traced),
      ),
    ).toEqual({
      record: "retired",
      streams: [
        {
          output: "@streamsy/accepted",
          id: Accepted.at({ ...params, epoch: 1 }).id,
          result: "removed",
        },
        { output: "ARoute", id: mid, result: "removed" },
        { output: "ARoute", id: alpha, result: "removed" },
        { output: "ARoute", id: zeta, result: "removed" },
        { output: "aOwned", id: Numbers.at({ ...params, rev: 1 }).id, result: "removed" },
        { output: "shared", id: Rows.at({ ...params, rev: 1 }).id, result: "shared" },
        { output: "zRoute", id: first, result: "removed" },
      ],
    });
    expect(removed).toEqual([mid, alpha, zeta, first]);
    expect((yield* nums.head(plainParams)).nextOffset).toBe(ZERO_OFFSET);
    expect((yield* (yield* Rows).head({ ...params, rev: 1 })).closed).toBe(false);
  }).pipe(Effect.provide(withFamilies(layerSqlite(), Source, Output, Numbers, Rows, Accepted))),
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
  expect(() => Projection.process(producer)).toThrow("Routed outputs require Commit.transaction");

  const emitter = { ...definition, emit: () => ({}) };
  // @ts-expect-error Routed output requires a process handler.
  expect(() => Projection.emit(emitter)).toThrow(/require Projection.process/);
});
