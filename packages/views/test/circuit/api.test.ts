/* oxlint-disable effecttsgo/strict-effect-provide -- Each test is an entry point with its own in-memory host. */
import { expect, it } from "@effect/vitest";
import { Array, Context, Effect, Layer, Option, Schedule, Schema } from "effect";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import {
  CheckpointStore,
  Projection,
  ProjectionWakeup,
  StateTable,
  Transaction,
} from "@streamsy/projection";
import { AlarmSlot } from "../../../serve/src/cloudflare/alarm.ts";
import { makeWakeups } from "../../../serve/src/cloudflare/wakeup.ts";
import { eventually } from "../../../projection/test/support/eventually.ts";
import { View, type TypedExpression } from "../../src/index.ts";

const Item = Schema.Struct({ id: Schema.String, value: Schema.Struct({ score: Schema.Finite }) });

const Details = Schema.Struct({
  id: Schema.String,
  value: Schema.Struct({ label: Schema.optionalKey(Schema.String), active: Schema.Boolean }),
});

const Result = Schema.Struct({
  id: Schema.String,
  label: Schema.String,
  original: Schema.optionalKey(Schema.String),
  absent: Schema.optionalKey(Schema.String),
});

class Source extends Streamsy.state<Source>()("ViewsTest/ApiSource", "view-api/source/:minimum", {
  params: { minimum: Schema.FiniteFromString },
  collections: { item: { schema: Item, key: "id" }, details: { schema: Details, key: "id" } },
}) {}

class Results extends Streamsy.state<Results>()(
  "ViewsTest/ApiResults",
  "view-api/results/:minimum",
  {
    params: { minimum: Schema.FiniteFromString },
    collections: { row: { schema: Result, key: "id" } },
  },
) {}

class Mirror extends Streamsy.state<Mirror>()("ViewsTest/ApiMirror", "view-api/mirror/:minimum", {
  params: Results.paramCodecs,
  collections: { row: { schema: Item, key: "id" } },
}) {}

const Table = StateTable.define(Results);

const MirrorTable = StateTable.define(Mirror);

const items = View.from(Source, "item");

const details = View.from(Source, "details");

const joined = items.leftJoin(details, {
  as: "details",
  on: (item, right) => item.id.eq(right.id),
});

const host = Projection.Registry.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(CheckpointStore.layer, Transaction.layer)),
  Layer.provideMerge(
    BunStorage.layer(Streamsy.group(Source, Results, Mirror), {
      client: { filename: ":memory:" },
    }),
  ),
);

const bindings = [{ minimum: 1 }, { minimum: 3 }];

const append = (
  entries: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Source.collections>>,
) =>
  Source.use((source) =>
    Effect.forEach(
      bindings,
      (params) => source.append(params, DurableState.changes(Source.at(params), {}, entries)),
      { discard: true },
    ),
  );

const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* View.migrate(sql);
  yield* StateTable.migrate(sql);
  const source = yield* Source;

  for (const params of bindings) yield* source.create(params);
  yield* append([
    DurableState.upsert("item", { id: "missing-alias", value: { score: 1 } }),
    DurableState.upsert("item", { id: "missing-leaf", value: { score: 2 } }),
    DurableState.upsert("item", { id: "present", value: { score: 3 } }),
    DurableState.upsert("item", { id: "hidden", value: { score: 4 } }),
    DurableState.upsert("details", { id: "missing-leaf", value: { active: true } }),
    DurableState.upsert("details", { id: "present", value: { label: "original", active: true } }),
    DurableState.upsert("details", { id: "hidden", value: { label: "hidden", active: false } }),
  ]);
});

const filter = View.fn(
  "active-by-minimum",
  1,
  (score: number, minimum: number, active: boolean | undefined) =>
    score >= minimum && active !== false,
);

const buildResult = (name: string, version: number) => {
  const label = View.fn(
    name,
    version,
    (detail: typeof Details.Type | undefined, value: string | undefined) =>
      value ?? (detail === undefined ? "no alias" : "no label"),
  );

  const absent = View.fn("omit", 1, (_value: string | undefined): string | undefined => undefined);

  return (minimum: TypedExpression<number>) =>
    joined
      .where((row) => filter(row.value.score, minimum, row.details.value.active))
      .select((row) => ({
        id: row.id,
        label: label(row.details, row.details.value.label),
        original: row.details.value.label,
        absent: absent(row.details.value.label),
      }));
};

const definition = { id: "api-circuit", params: Results.paramCodecs, output: Results };

const circuit = (name = "label", version = 1) =>
  View.circuit({
    ...definition,
    views: ({ minimum }) => ({ row: buildResult(name, version)(minimum) }),
  });

it.effect("resolves missing function arguments and binds decoded parameters per namespace", () =>
  Effect.gen(function* () {
    yield* seed;
    const declaration = circuit();

    for (const minimum of [1, 3]) {
      yield* Projection.run(declaration.at({ minimum }));
      yield* Projection.run(Table.at({ minimum }));
    }

    expect(
      Array.map((yield* Table.at({ minimum: 1 }).list("row")).rows, ({ value }) => value),
    ).toEqual([
      { id: "missing-alias", label: "no alias" },
      { id: "missing-leaf", label: "no label" },
      { id: "present", label: "original", original: "original" },
    ]);
    expect(
      Array.map((yield* Table.at({ minimum: 3 }).list("row")).rows, ({ value }) => value),
    ).toEqual([{ id: "present", label: "original", original: "original" }]);
    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT DISTINCT plan_hash FROM streamsy_view_circuits`).toHaveLength(1);

    yield* append([DurableState.remove("details", "present")]);
    yield* Projection.run(declaration.at({ minimum: 1 }));
    yield* Projection.run(Table.at({ minimum: 1 }));
    expect((yield* Table.at({ minimum: 1 }).get("row", "present")).value).toEqual({
      id: "present",
      label: "no alias",
    });

    for (const changed of [circuit("new-name"), circuit("label", 2)]) {
      const fault = yield* Projection.run(changed.at({ minimum: 3 })).pipe(Effect.flip);
      expect(fault).toBeInstanceOf(View.PlanChanged);
      expect(fault).toMatchObject({ stored: expect.any(String), requested: expect.any(String) });

      if (Schema.is(View.PlanChanged)(fault)) expect(fault.stored).not.toBe(fault.requested);
    }
  }).pipe(Effect.provide(host)),
);

const namedParameterCircuit = (name: "minimum" | "other") =>
  View.circuit({
    ...definition,
    params: { ...Results.paramCodecs, other: Schema.FiniteFromString },
    views: (params) => ({ row: buildResult("label", 1)(params[name]) }),
  });

it.effect("parameter reference names change the plan hash", () =>
  Effect.gen(function* () {
    yield* seed;

    const params = { minimum: 1, other: 1 };
    yield* Projection.run(namedParameterCircuit("minimum").at(params));
    yield* append([DurableState.remove("details", "present")]);
    expect(
      yield* Projection.run(namedParameterCircuit("other").at(params)).pipe(Effect.flip),
    ).toBeInstanceOf(View.PlanChanged);
  }).pipe(Effect.provide(host)),
);

it.effect(
  "owns multiple outputs with independent collection names and commits a wake deadline",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(0);
      yield* seed;

      const declaration = View.circuit({
        id: "api-multi",
        params: Results.paramCodecs,
        outputs: { selected: Results, original: Mirror },
        views: ({ minimum }) => ({
          selected: { row: buildResult("label", 1)(minimum) },
          original: { row: items },
        }),
        wakeAt: (unit) => (Option.isNone(unit.due) ? Projection.wakeAt(100) : undefined),
      });

      const params = { minimum: 1 };
      expect((yield* Projection.run(declaration.at(params))).wakeAt).toEqual(Option.some(100));
      yield* Projection.run(Table.at(params));
      yield* Projection.run(MirrorTable.at(params));
      expect((yield* Table.at(params).list("row")).rows).toHaveLength(3);
      expect((yield* MirrorTable.at(params).list("row")).rows).toHaveLength(4);
      yield* TestClock.adjust(100);
      const wake = yield* Projection.run(declaration.at(params));
      expect(wake).toMatchObject({ units: 1, items: 0, wakeAt: Option.none() });
      expect((yield* Projection.run(declaration.at(params))).units).toBe(0);

      const mirror = yield* Mirror;
      const tail = (yield* mirror.head(params)).nextOffset;
      const checkpoint = yield* Projection.read(declaration.at(params));
      yield* (yield* Results).append(params, [], { close: true });
      yield* append([DurableState.upsert("item", { id: "new", value: { score: 5 } })]);
      yield* Projection.run(declaration.at(params)).pipe(Effect.flip);
      expect((yield* mirror.head(params)).nextOffset).toBe(tail);
      expect(yield* Projection.read(declaration.at(params))).toEqual(checkpoint);
    }).pipe(Effect.provide(host)),
);

it.live("creates every output on the first unit and recreates a missing output on append", () =>
  Effect.gen(function* () {
    yield* seed;
    const params = bindings[0]!;
    const binding = View.circuit({
      id: "api-empty-output",
      params: Results.paramCodecs,
      outputs: { selected: Results, original: Mirror },
      views: ({ minimum }) => ({
        selected: { row: buildResult("empty", 1)(minimum).where((row) => row.id.eq("new")) },
        original: { row: items },
      }),
    }).at(params);

    yield* Projection.run(binding);
    const selected = yield* Results;
    const original = yield* Mirror;
    expect((yield* selected.head(params)).nextOffset).toBe(ZERO_OFFSET);
    yield* original.remove(params);
    yield* append([DurableState.upsert("item", { id: "new", value: { score: 5 } })]);
    expect((yield* Projection.run(binding)).units).toBe(1);
    yield* Projection.run(Table.at(params));
    yield* Projection.run(MirrorTable.at(params));
    expect((yield* Table.at(params).get("row", "new")).value?.id).toBe("new");
    expect((yield* MirrorTable.at(params).get("row", "new")).value?.id).toBe("new");
  }).pipe(Effect.provide(host)),
);

it("rejects partial and duplicate output coverage at declaration time", () => {
  expect(() => View.circuit({ ...definition, output: Source, views: { item: items } })).toThrow(
    "Missing view for output collection",
  );
  expect(() =>
    View.circuit({
      id: "partial",
      params: Results.paramCodecs,
      outputs: { a: Mirror, b: Results },
      views: { a: { row: items }, b: {} },
    }),
  ).toThrow("Missing view for output collection");
  expect(() =>
    View.circuit({
      id: "duplicate",
      params: Results.paramCodecs,
      outputs: { a: Mirror, b: Mirror },
      views: { a: { row: items }, b: { row: items } },
    }),
  ).toThrow("Duplicate output State class");
});

for (const alarm of [false, true]) {
  for (const changedPlan of [false, true]) {
    it.effect(`permanent circuit faults stop (alarm=${alarm}, changedPlan=${changedPlan})`, () =>
      Effect.gen(function* () {
        yield* TestClock.setTime(0);
        yield* seed;
        let steps = 0;
        const minimum = 1;

        const retry = {
          schedule: Schedule.spaced("1 millis").pipe(
            Schedule.modifyDelay(({ duration }) =>
              Effect.sync(() => {
                steps += 1;

                return duration;
              }),
            ),
          ),
        };

        const bad = View.circuit({
          ...definition,
          retry,
          views: (params) => ({
            row: changedPlan
              ? buildResult("changed", 1)(params.minimum)
              : joined.select((row) => ({
                  id: row.id,
                  label: row.details.unwrap().value.label.orElse("missing"),
                })),
          }),
        });

        const binding = bad.at({ minimum });

        if (changedPlan) {
          yield* Projection.run(circuit().at({ minimum }));
          yield* append([DurableState.remove("details", "present")]);
        }

        const before = yield* Projection.read(binding);
        const registry = yield* Projection.Registry;
        const context = yield* Effect.context<Layer.Success<typeof host>>();

        const scheduler = alarm
          ? yield* makeWakeups(
              Context.add(context, AlarmSlot, {
                current: Effect.succeedNone,
                arm: () => Effect.void,
                clear: Effect.void,
              }),
              [ProjectionWakeup.attach(bad, { durable: true })],
            )
          : undefined;

        if (!alarm) yield* Layer.build(Projection.attach(bad, { durable: true }));
        yield* registry.schedule(binding);

        if (scheduler) yield* scheduler.alarm;

        const failed = yield* eventually(
          registry.failed(binding),
          (entries) => entries[0]?.stopped === true,
        );

        expect(failed[0]).toMatchObject({
          attempts: 1,
          stopped: true,
          exhausted: false,
          dueAt: Option.none(),
          failure: Option.some({
            reason: Option.some("non-retryable-handler"),
            message: expect.stringContaining(
              changedPlan ? "Circuit plan changed" : "cannot unwrap an absent value",
            ),
          }),
        });
        yield* append([DurableState.upsert("item", { id: "later", value: { score: 5 } })]);
        yield* TestClock.adjust(60_000);

        if (scheduler) yield* scheduler.alarm;
        expect(steps).toBe(0);
        expect((yield* registry.failed(binding))[0]?.attempts).toBe(1);
        expect(yield* Projection.read(binding)).toEqual(before);
      }).pipe(Effect.provide(host)),
    );
  }
}
