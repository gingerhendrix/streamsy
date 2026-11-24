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
import {
  PlanChanged,
  PlanCheckFailed,
  View,
  ViewFault,
  type TypedExpression,
} from "../../src/index.ts";
import { parameterReference } from "../../src/expression.ts";

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
  "view-api/results/:minimum/g/:generation",
  {
    params: { ...Source.paramCodecs, generation: Schema.FiniteFromString },
    collections: { row: { schema: Result, key: "id" } },
  },
) {}

class Mirror extends Streamsy.state<Mirror>()(
  "ViewsTest/ApiMirror",
  "view-api/mirror/:minimum/g/:generation",
  {
    params: Results.paramCodecs,
    collections: { row: { schema: Item, key: "id" } },
  },
) {}

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

const definition = {
  id: "api-circuit",
  params: Source.paramCodecs,
  output: Projection.perGeneration(Results, "generation"),
};

const circuit = (name = "label", version = 1) =>
  View.circuit({
    ...definition,
    views: ({ minimum }) => ({ row: buildResult(name, version)(minimum) }),
  });

it("rejects a legacy circuit version at declaration", () => {
  const result = buildResult("label", 1);

  const declare = (version: number | undefined) => {
    const legacy = { ...definition, version };

    return View.circuit({
      ...legacy,
      views: ({ minimum }) => ({ row: result(minimum) }),
    });
  };

  expect(() => declare(3)).toThrow(
    new RangeError(
      "Projection api-circuit: version was removed. Raise generation above every generation the projection has used.",
    ),
  );
  expect(declare(undefined).generation).toBe(1);
});

it("rejects legacy versions in multi-output circuit declarations", () => {
  const result = buildResult("label", 1);

  const declare = (version: number | undefined) => {
    const legacy = { version };

    return View.circuit({
      ...legacy,
      id: definition.id,
      params: definition.params,
      outputs: {
        result: definition.output,
        mirror: Projection.perGeneration(Mirror, "generation"),
      },
      views: ({ minimum }) => ({ result: { row: result(minimum) }, mirror: { row: items } }),
    });
  };

  expect(() => declare(1)).toThrow(
    new RangeError(
      "Projection api-circuit: version was removed. Remove version: 1 from the declaration.",
    ),
  );
  expect(() => declare(3)).toThrow(
    new RangeError(
      "Projection api-circuit: version was removed. Raise generation above every generation the projection has used.",
    ),
  );
  expect(declare(undefined).generation).toBe(1);
});

it("rejects invalid joins, aggregates, top bounds and parameter references from View.from", () => {
  const invalid = items
    .join(details, { as: "", on: (item, right) => item.id.ne(right.id) })
    .where((row) => row.value.score.gt(parameterReference<number>("other")))
    .top({ by: () => [], limit: 0 })
    .groupBy(() => ({}))
    .aggregate(() => ({}))
    .select(() => ({ id: View.literal("id"), value: View.literal({ score: 1 }) }));

  const { cause } = Effect.runSync(
    Effect.flip(
      Effect.try(() =>
        View.circuit({
          id: "api-invalid",
          params: Source.paramCodecs,
          output: Projection.perGeneration(Mirror, "generation"),
          views: () => ({ row: invalid }),
        }),
      ),
    ),
  );

  expect(cause).toBeInstanceOf(PlanCheckFailed);

  if (!Schema.is(PlanCheckFailed)(cause)) throw new Error("Expected circuit plan issues");
  expect(Array.map(cause.issues, (issue) => issue.code)).toEqual(
    expect.arrayContaining([
      "join-alias-conflict",
      "unsupported-sql-plan",
      "undeclared-parameter",
      "invalid-top-limit",
      "unstable-top-order",
      "aggregate-shape-mismatch",
    ]),
  );
});

it.effect("resolves missing function arguments and binds decoded parameters per namespace", () =>
  Effect.gen(function* () {
    yield* seed;
    const declaration = circuit();

    for (const minimum of [1, 3]) {
      yield* Projection.run(declaration.at({ minimum }));
      yield* Projection.run(Table.at({ minimum, generation: 1 }));
    }

    expect(
      Array.map(
        (yield* Table.at({ minimum: 1, generation: 1 }).list("row")).rows,
        ({ value }) => value,
      ),
    ).toEqual([
      { id: "missing-alias", label: "no alias" },
      { id: "missing-leaf", label: "no label" },
      { id: "present", label: "original", original: "original" },
    ]);
    expect(
      Array.map(
        (yield* Table.at({ minimum: 3, generation: 1 }).list("row")).rows,
        ({ value }) => value,
      ),
    ).toEqual([{ id: "present", label: "original", original: "original" }]);
    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT DISTINCT plan_hash FROM streamsy_view_circuits`).toHaveLength(1);

    yield* append([DurableState.remove("details", "present")]);
    yield* Projection.run(declaration.at({ minimum: 1 }));
    yield* Projection.run(Table.at({ minimum: 1, generation: 1 }));
    expect((yield* Table.at({ minimum: 1, generation: 1 }).get("row", "present")).value).toEqual({
      id: "present",
      label: "no alias",
    });

    for (const changed of [circuit("new-name"), circuit("label", 2)]) {
      const fault = yield* Projection.run(changed.at({ minimum: 3 })).pipe(Effect.flip);
      expect(fault).toBeInstanceOf(PlanChanged);
      expect(fault).toMatchObject({ stored: expect.any(String), requested: expect.any(String) });

      if (Schema.is(PlanChanged)(fault)) expect(fault.stored).not.toBe(fault.requested);
    }
  }).pipe(Effect.provide(host)),
);

it.effect("stores the same plan hash for equivalent output and outputs declarations", () =>
  Effect.gen(function* () {
    yield* seed;

    const multiple = View.circuit({
      id: definition.id,
      params: definition.params,
      outputs: { output: Projection.perGeneration(Results, "generation") },
      views: ({ minimum }) => ({ output: { row: buildResult("label", 1)(minimum) } }),
    });

    yield* Projection.run(circuit().at({ minimum: 1 }));
    yield* Projection.run(multiple.at({ minimum: 3 }));

    const sql = yield* SqlClient.SqlClient;
    const stored = yield* sql<{ plan_hash: string }>`SELECT plan_hash FROM streamsy_view_circuits`;
    expect(stored).toHaveLength(2);
    expect(stored[0]?.plan_hash).toMatch(/^[0-9a-f]{8}$/);
    expect(stored[0]?.plan_hash).toBe(stored[1]?.plan_hash);
  }).pipe(Effect.provide(host)),
);

it.effect("projects a row from a whole left-joined alias and a binding parameter", () =>
  Effect.gen(function* () {
    yield* seed;

    const project = View.fn(
      "project-item",
      1,
      (id: string, detail: typeof Details.Type | undefined, minimum: number) => ({
        id,
        label: `${minimum}: ${detail === undefined ? "no alias" : (detail.value.label ?? "no label")}`,
      }),
    );

    const declaration = View.circuit({
      ...definition,
      views: ({ minimum }) => ({
        row: joined.selectRow((row) => project(row.id, row.details, minimum)),
      }),
    });

    const minimum = 3;
    const table = Table.at({ minimum, generation: 1 });

    yield* Projection.run(declaration.at({ minimum }));
    yield* Projection.run(table);

    expect((yield* table.get("row", "missing-alias")).value).toEqual({
      id: "missing-alias",
      label: "3: no alias",
    });
    expect((yield* table.get("row", "present")).value).toEqual({
      id: "present",
      label: "3: original",
    });
  }).pipe(Effect.provide(host)),
);

const namedParameterCircuit = (name: "minimum" | "other") =>
  View.circuit({
    ...definition,
    params: { ...Source.paramCodecs, other: Schema.FiniteFromString },
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
    ).toBeInstanceOf(PlanChanged);
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
        params: Source.paramCodecs,
        outputs: {
          selected: Projection.perGeneration(Results, "generation"),
          original: Projection.perGeneration(Mirror, "generation"),
        },
        views: ({ minimum }) => ({
          selected: { row: buildResult("label", 1)(minimum) },
          original: { row: items },
        }),
        wakeAt: (unit) => (Option.isNone(unit.due) ? Projection.wakeAt(100) : undefined),
      });

      const params = { minimum: 1 };
      expect((yield* Projection.run(declaration.at(params))).wakeAt).toEqual(Option.some(100));
      yield* Projection.run(Table.at({ ...params, generation: 1 }));
      yield* Projection.run(MirrorTable.at({ ...params, generation: 1 }));
      expect((yield* Table.at({ ...params, generation: 1 }).list("row")).rows).toHaveLength(3);
      expect((yield* MirrorTable.at({ ...params, generation: 1 }).list("row")).rows).toHaveLength(
        4,
      );
      yield* TestClock.adjust(100);
      const wake = yield* Projection.run(declaration.at(params));
      expect(wake).toMatchObject({ units: 1, items: 0, wakeAt: Option.none() });
      expect((yield* Projection.run(declaration.at(params))).units).toBe(0);

      const mirror = yield* Mirror;
      const tail = (yield* mirror.head({ ...params, generation: 1 })).nextOffset;
      const checkpoint = yield* Projection.read(declaration.at(params));
      yield* (yield* Results).append({ ...params, generation: 1 }, [], { close: true });
      yield* append([DurableState.upsert("item", { id: "new", value: { score: 5 } })]);
      yield* Projection.run(declaration.at(params)).pipe(Effect.flip);
      expect((yield* mirror.head({ ...params, generation: 1 })).nextOffset).toBe(tail);
      expect(yield* Projection.read(declaration.at(params))).toEqual(checkpoint);
    }).pipe(Effect.provide(host)),
);

it.live(
  "creates empty outputs on the first unit and fails if a later append finds one missing",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const params = bindings[0]!;
      const outputParams = { ...params, generation: 1 };

      const binding = View.circuit({
        id: "api-empty-output",
        params: Source.paramCodecs,
        outputs: {
          selected: Projection.perGeneration(Results, "generation"),
          original: Projection.perGeneration(Mirror, "generation"),
        },
        views: ({ minimum }) => ({
          selected: { row: buildResult("empty", 1)(minimum).where((row) => row.id.eq("new")) },
          original: { row: items },
        }),
      }).at(params);

      yield* Projection.run(binding);
      const selected = yield* Results;
      const original = yield* Mirror;
      expect((yield* selected.head(outputParams)).nextOffset).toBe(ZERO_OFFSET);
      const checkpoint = yield* Projection.read(binding);
      yield* original.remove(outputParams);
      yield* append([DurableState.upsert("item", { id: "new", value: { score: 5 } })]);

      expect(yield* Projection.run(binding).pipe(Effect.flip)).toMatchObject({
        phase: "process",
        reason: "invalid-output",
      });
      expect(yield* Projection.read(binding)).toEqual(checkpoint);
      expect((yield* selected.head(outputParams)).nextOffset).toBe(ZERO_OFFSET);
      expect(yield* original.head(outputParams).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
    }).pipe(Effect.provide(host)),
);

it("rejects unwrapped State classes in both output forms", () => {
  const single = { ...definition, output: Mirror, views: { row: items } };

  const multiple = {
    id: "unwrapped-multi",
    params: definition.params,
    outputs: { original: Mirror },
    views: { original: { row: items } },
  };

  // @ts-expect-error an unwrapped output is invalid
  expect(() => View.circuit(single)).toThrow(ViewFault);
  // @ts-expect-error each output must be wrapped
  expect(() => View.circuit(multiple)).toThrow(ViewFault);
});

it("rejects partial and duplicate output coverage at declaration time", () => {
  expect(() =>
    View.circuit({
      ...definition,
      output: Projection.perGeneration(Results, "generation"),
      views: {},
    }),
  ).toThrow("Missing view for output collection");
  expect(() =>
    View.circuit({
      id: "partial",
      params: Source.paramCodecs,
      outputs: {
        a: Projection.perGeneration(Mirror, "generation"),
        b: Projection.perGeneration(Results, "generation"),
      },
      views: { a: { row: items }, b: {} },
    }),
  ).toThrow("Missing view for output collection");
  expect(() =>
    View.circuit({
      id: "duplicate",
      params: Source.paramCodecs,
      outputs: {
        a: Projection.perGeneration(Mirror, "generation"),
        b: Projection.perGeneration(Mirror, "generation"),
      },
      views: { a: { row: items }, b: { row: items } },
    }),
  ).toThrow("Duplicate output State class");
});

for (const alarm of [false, true]) {
  for (const fault of ["plan", "data", "fan-out"] as const) {
    it.effect(`permanent circuit faults stop (alarm=${alarm}, fault=${fault})`, () =>
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
          maxAffectedKeys: fault === "fan-out" ? 1 : undefined,
          views: (params) => ({
            row:
              fault === "plan"
                ? buildResult("changed", 1)(params.minimum)
                : joined.select((row) => ({
                    id: row.id,
                    label: row.details.unwrap().value.label.orElse("missing"),
                  })),
          }),
        });

        const binding = bad.at({ minimum });

        if (fault === "plan") {
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
              {
                plan: "Circuit plan changed",
                data: "cannot unwrap an absent value",
                "fan-out": "maxAffectedKeys (1) exceeded at key",
              }[fault],
            ),
          }),
        });
        yield* append([DurableState.upsert("item", { id: "later", value: { score: 5 } })]);
        yield* TestClock.adjust(60_000);

        if (scheduler) yield* scheduler.alarm;
        expect(steps).toBe(0);
        expect((yield* registry.failed(binding))[0]?.attempts).toBe(1);
        expect(yield* Projection.read(binding)).toEqual(before);

        if (fault === "fan-out") {
          const output = yield* Results;

          expect(yield* output.head({ minimum, generation: 1 }).pipe(Effect.flip)).toMatchObject({
            _tag: "StreamNotFound",
          });
        }
      }).pipe(Effect.provide(host)),
    );
  }
}
