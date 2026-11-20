import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array, Effect, Option } from "effect";
import { SqlClient } from "effect/sql";
import { canonicalJson } from "../../src/runtime/key.ts";
import { storeFor } from "../../src/sql/store.ts";
import { fullRecompute, makeEngine, migrate, type StateChange } from "../../src/sql/engine.ts";
import { plan, ref, schema, shopPlans, source } from "./fixtures.ts";

const order = (orderId: string, total: number, customerId = "c1") => ({
  orderId,
  customerId,
  total,
});

const upsert = (value: ReturnType<typeof order>): StateChange => ({
  sourceId: "orders",
  kind: "upsert",
  key: value.orderId,
  value,
});

const remove = (key: string): StateChange => ({ sourceId: "orders", kind: "delete", key });

const byCustomer = plan("byCustomer", [
  source("orders", "orderId"),
  { kind: "key", id: "keyed", schema, input: "orders", key: ref("row", "customerId") },
]);

it.live("retains the union of direct filters unless another view needs the whole source", () =>
  Effect.gen(function* () {
    yield* migrate();
    const input = source("orders", "orderId");

    const filtered = (name: string, total: number) =>
      plan(name, [
        input,
        {
          kind: "filter",
          id: name,
          schema,
          input: input.id,
          predicate: {
            kind: "binary",
            operator: "equal",
            left: ref("row", "total"),
            right: { kind: "literal", value: total },
          },
        },
      ]);

    const views = { a: filtered("a", 1), b: filtered("b", 2) };
    const pushed = yield* makeEngine(views);
    const shared = yield* makeEngine({ ...views, raw: plan("raw", [input]) });
    const sql = yield* SqlClient.SqlClient;
    const seed = [upsert(order("o1", 1)), upsert(order("o2", 2)), upsert(order("o3", 3))];
    yield* pushed.process("pushed", seed);
    yield* shared.process("shared", seed);
    expect(Array.map(yield* storeFor(sql, "pushed").rows("r0"), ({ key }) => key)).toEqual([
      "o1",
      "o2",
    ]);
    expect(Array.map(yield* storeFor(sql, "shared").rows("r0"), ({ key }) => key)).toEqual([
      "o1",
      "o2",
      "o3",
    ]);

    const change = [upsert(order("o1", 2))];

    const expected = [
      { view: "a", kind: "delete", key: "o1" },
      { view: "b", kind: "upsert", key: "o1", value: order("o1", 2) },
    ];

    expect(yield* pushed.process("pushed", change)).toEqual(expected);
    expect(yield* shared.process("shared", change)).toEqual([
      ...expected,
      { view: "raw", kind: "upsert", key: "o1", value: order("o1", 2) },
    ]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("rejects a second owner in the same or a later unit and rolls back the failed unit", () =>
  Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine({ byCustomer });
    const one = order("o1", 1);
    const two = order("o2", 2);
    yield* engine.process("probe", [upsert(one)]);
    const result = yield* engine.process("probe", [upsert(two)]).pipe(Effect.result);
    expect(result).toMatchObject({
      _tag: "Failure",
      failure: {
        _tag: "ViewDataFault",
        view: "byCustomer",
        node: "keyed",
        key: "c1",
        reason: "duplicate output key",
      },
    });
    expect(yield* engine.process("probe", [remove("o2")])).toEqual([]);
    expect(yield* engine.process("probe", [remove("o1")])).toEqual([
      { view: "byCustomer", kind: "delete", key: "c1" },
    ]);
    expect(
      yield* engine.process("same-unit", [upsert(one), upsert(two)]).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure", failure: { _tag: "ViewDataFault", key: "c1" } });
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it("rejects duplicates in the full recompute oracle", () => {
  expect(() =>
    fullRecompute({
      plan: byCustomer,
      sources: {
        orders: [
          { key: "o1", row: order("o1", 1) },
          { key: "o2", row: order("o2", 2) },
        ],
      },
    }),
  ).toThrow(/duplicate.*key/i);
});

it("rejects wire key collisions when recomputing State outputs", () => {
  const input = {
    plan: byCustomer,
    sources: {
      orders: [
        { key: "o1", row: { ...order("o1", 1), customerId: 1 } },
        { key: "o2", row: { ...order("o2", 2), customerId: "1" } },
      ],
    },
  };

  expect(fullRecompute(input).rows).toHaveLength(2);
  expect(() => fullRecompute(input, true)).toThrow(/duplicate.*key/i);
});

it.live("matches a full recompute after removing one fractional operand", () =>
  Effect.gen(function* () {
    yield* migrate();
    const totals = shopPlans().totals;

    if (!totals) throw new Error("missing totals fixture");
    const engine = yield* makeEngine({ totals });
    const a = order("o1", 0.1);
    const b = order("o2", 0.2);
    yield* engine.process("float", [upsert(a), upsert(b)]);
    const changes = yield* engine.process("float", [remove("o1")]);
    const expected = fullRecompute({ plan: totals, sources: { orders: [{ key: "o2", row: b }] } });
    expect(changes).toEqual(
      Array.map(expected.rows, ({ key, row }) => ({
        view: "totals",
        kind: "upsert",
        key,
        value: row,
      })),
    );
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("transfers and swaps ownership from the final unit image", () =>
  Effect.gen(function* () {
    yield* migrate();

    const projected = plan("projected", [
      source("orders", "orderId"),
      {
        kind: "project",
        id: "projected",
        schema,
        input: "orders",
        fields: { customerId: ref("row", "customerId"), total: ref("row", "total") },
      },
      { kind: "key", id: "keyed", schema, input: "projected", key: ref("row", "customerId") },
    ]);

    const engine = yield* makeEngine({ projected });
    yield* engine.process("transfer", [upsert(order("o1", 1)), upsert(order("o3", 3, "c2"))]);
    // o2 replaces o1 with identical payload, even though the claim arrives first.
    expect(yield* engine.process("transfer", [upsert(order("o2", 1)), remove("o1")])).toEqual([]);
    expect(
      yield* engine.process("transfer", [upsert(order("o1", 1))]).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure", failure: { _tag: "ViewDataFault", key: "c1" } });

    expect(
      yield* engine.process("transfer", [upsert(order("o2", 1, "c2")), upsert(order("o3", 3))]),
    ).toEqual([
      { view: "projected", key: "c1", kind: "upsert", value: { customerId: "c1", total: 3 } },
      { view: "projected", key: "c2", kind: "upsert", value: { customerId: "c2", total: 1 } },
    ]);
    expect(yield* engine.process("transfer", [upsert(order("o3", 4))])).toEqual([
      { view: "projected", key: "c1", kind: "upsert", value: { customerId: "c1", total: 4 } },
    ]);
    expect(yield* engine.process("transfer", [remove("o3")])).toEqual([
      { view: "projected", key: "c1", kind: "delete" },
    ]);
    expect(yield* engine.process("transfer", [upsert(order("o1", 1))])).toHaveLength(1);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live(
  "uses current group membership and canonical order despite cancellation and replay order",
  () =>
    Effect.gen(function* () {
      yield* migrate();
      const totals = shopPlans().totals;

      if (!totals) throw new Error("missing totals fixture");
      const engine = yield* makeEngine({ totals });
      const a = order("a", 10 ** 16);
      const b = order("b", 0.1);
      const c = order("c", -(10 ** 16));
      const direct = yield* engine.process("direct", [upsert(c), upsert(a), upsert(b)]);
      yield* engine.process("staged", [upsert(a), upsert(c)]);
      const staged = yield* engine.process("staged", [upsert(b)]);
      expect(staged).toEqual(direct);
      expect(direct[0]).toMatchObject({ value: { revenue: 0 } });

      for (const namespace of ["direct", "staged"]) {
        const changes = yield* engine.process(namespace, [remove("a"), remove("c")]);
        expect(changes[0]).toMatchObject({ value: { revenue: 0.1 } });
      }
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("rebuilds R1 owner state and corrects stored R1 totals from the input integrals", () =>
  Effect.gen(function* () {
    yield* migrate();
    const totals = shopPlans().totals;

    if (!totals) throw new Error("missing totals fixture");
    const sql = yield* SqlClient.SqlClient;
    const engine = yield* makeEngine({ byCustomer, totals });
    const input = Array.findFirst(engine.program.nodes, (node) => node.kind === "source");

    const aggregate = Array.findFirst(
      engine.program.nodes,
      (node) => node.kind === "grouped-aggregate",
    );

    if (Option.isNone(input) || Option.isNone(aggregate))
      throw new Error("missing compiled fixture");
    const b = order("o2", 0.2);
    const store = storeFor(sql, "r1");
    yield* store.put(input.value.id, [{ key: b.orderId, row: b }]);
    yield* store.accumulatorPut(aggregate.value.id, canonicalJson(["c1"]), {
      count: 1,
      sums: { revenue: 0.20000000000000004 },
    });

    expect(
      yield* engine.process("r1", [upsert(order("o1", 0.1))]).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure", failure: { _tag: "ViewDataFault", key: "c1" } });
    expect(yield* store.get("@layout", "s2:r2")).toBeUndefined();
    expect(yield* engine.process("r1", [])).toEqual([
      {
        view: "totals",
        key: ["c1"],
        kind: "upsert",
        value: { customerId: "c1", orders: 1, revenue: 0.2, largest: 0.2 },
      },
    ]);
    expect(yield* engine.process("r1", [])).toEqual([]);
    expect(
      yield* engine.process("r1", [upsert(order("o1", 0.1))]).pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure", failure: { _tag: "ViewDataFault", view: "byCustomer" } });
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("reports source and aggregate data faults with the offending input key", () =>
  Effect.gen(function* () {
    yield* migrate();
    const totals = shopPlans().totals;

    if (!totals) throw new Error("missing totals fixture");
    const engine = yield* makeEngine({ totals });
    const invalid = { orderId: "o1", customerId: "c1", total: "bad" };
    expect(
      yield* engine
        .process("invalid", [{ sourceId: "orders", key: "other", kind: "upsert", value: invalid }])
        .pipe(Effect.result),
    ).toMatchObject({
      _tag: "Failure",
      failure: {
        _tag: "ViewDataFault",
        view: "totals",
        node: "orders",
        key: "other",
        reason: "input key disagrees with source key expression",
      },
    });
    // The max arrangement accepts strings, so the numeric aggregate supplies this fault.
    expect(
      yield* engine
        .process("invalid", [{ sourceId: "orders", key: "o1", kind: "upsert", value: invalid }])
        .pipe(Effect.result),
    ).toMatchObject({
      _tag: "Failure",
      failure: {
        _tag: "ViewDataFault",
        view: "totals",
        node: "totals",
        key: "o1",
        reason: "sum requires finite numbers",
      },
    });
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("skips ownership for a source key through top and a whole composite group key", () =>
  Effect.gen(function* () {
    yield* migrate();
    const { topOrders, totals } = shopPlans();

    if (!topOrders || !totals) throw new Error("missing compiled fixtures");

    const top = plan("keyedTop", [
      ...topOrders.nodes,
      { kind: "key", id: "keyed", schema, input: topOrders.output, key: ref("row", "orderId") },
    ]);

    const group = plan("keyedGroup", [
      ...totals.nodes,
      {
        kind: "key",
        id: "keyed",
        schema,
        input: totals.output,
        key: { kind: "variadic", operator: "key", operands: [ref("row", "customerId")] },
      },
    ]);

    const engine = yield* makeEngine({ top, group });

    expect(engine.program.checkedKeys.size).toBe(0);
    yield* engine.process("proven", [upsert(order("o1", 1))]);
    const sql = yield* SqlClient.SqlClient;
    const owners = yield* sql`SELECT * FROM streamsy_view_key_owners`;
    expect(owners).toEqual([]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);
