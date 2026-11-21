import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array as Arr, Effect, Exit, Order, Record as Rec, Schema } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { View, ViewDataFault } from "../../src/index.ts";
import { collectPlanIssues } from "../../src/check.ts";
import { compilePlan } from "../../src/relation.ts";
import { keyExpression } from "../../src/expression.ts";
import { circuitPlanHash } from "../../src/plan.ts";
import { makeEngine, migrate, type StateChange } from "../../src/sql/engine.ts";
import type { ClosureNode, JsonObject, RelationNode } from "../../src/ir/contracts.ts";
import { scalarKey } from "../../src/sql/encoding.ts";
import { canonicalJson, encodeRowKey } from "../../src/runtime/key.ts";
import { fullRecompute } from "../support/full-recompute.ts";
import { identity, materialize } from "./support.ts";
import { plan, ref, shopPlans, source } from "./fixtures.ts";

const up = (sourceId: string, key: string, value: JsonObject): StateChange => ({
  sourceId,
  key,
  kind: "upsert",
  value,
});

const del = (sourceId: string, key: string): StateChange => ({ sourceId, key, kind: "delete" });

const order = (id: string, customerId = "c", total = 1) =>
  up("orders", id, { orderId: id, customerId, total });

const customer = (id: string, tier = "gold") =>
  up("customers", id, { customerId: id, name: id, tier });

const aggregateItem = (id: string, v?: number) =>
  up("items", id, v === undefined ? { id, group: "g" } : { id, group: "g", v });

const rankedItem = (id: string, v: number, group = "g") => up("items", id, { id, v, group });

const closureNode = (maxDepth: number): ClosureNode => ({
  kind: "closure",
  id: "closure",
  input: "nodes",
  parent: ref("row", "parent"),
  part: ref("row", "part"),
  maxDepth,
});

const edge = (id: string, parent: string, part = id) => up("nodes", id, { id, parent, part });

const expectedPath = (node: string, ancestor: string, ...path: string[]) => ({
  view: "lineage",
  kind: "upsert",
  key: [encodeRowKey(node), encodeRowKey(ancestor)],
  value: { node, ancestor, depth: path.length, path },
});

const lineagePlan = (node: ClosureNode) => plan("lineage", [source("nodes"), node]);

const removedPath = (node: string, ancestor: string) => ({
  view: "lineage",
  kind: "delete",
  key: [encodeRowKey(node), encodeRowKey(ancestor)],
});

it("checks closure depth, expression scopes and string keys and hashes the closure spec", () => {
  for (const maxDepth of [-1, 0, 0.5, Infinity, NaN]) {
    expect(collectPlanIssues(lineagePlan(closureNode(maxDepth)))).toMatchObject([
      { code: "invalid-closure-depth", path: "nodes[1].maxDepth" },
    ]);
  }

  expect(
    collectPlanIssues(
      lineagePlan({ ...closureNode(1), parent: ref("left", "p"), part: ref("right", "name") }),
    ),
  ).toMatchObject([
    { code: "invalid-expression-scope", path: "nodes[1].parent" },
    { code: "invalid-expression-scope", path: "nodes[1].part" },
  ]);

  expect(
    collectPlanIssues(lineagePlan({ ...closureNode(1), parent: View.literal(42) })),
  ).toMatchObject([{ code: "invalid-closure-key", path: "nodes[1].parent" }]);
  expect(
    collectPlanIssues(
      plan("lineage", [
        source("nodes"),
        { kind: "key", id: "composite", input: "nodes", key: keyExpression(["id", "parent"]) },
        { ...closureNode(1), input: "composite" },
      ]),
    ),
  ).toMatchObject([{ code: "invalid-closure-key", path: "nodes[2].input" }]);

  const node = closureNode(1);
  const hash = (closure: ClosureNode) => circuitPlanHash({ lineage: lineagePlan(closure) });
  expect(hash(node)).not.toBe(hash(closureNode(2)));
  expect(hash(node)).not.toBe(hash({ ...node, part: View.literal("part") }));
  expect(hash(node)).not.toBe(hash({ ...node, parent: View.literal("root") }));
});

it.live(
  "closure incrementally updates paths on edge edits and keeps absent parents as ancestors",
  () =>
    Effect.gen(function* () {
      yield* migrate();

      const engine = yield* makeEngine({
        lineage: lineagePlan(closureNode(3)),
      });

      const run = (changes: readonly StateChange[]) => engine.process("closure", changes);

      expect(yield* run([edge("b", "a"), edge("c", "b")])).toEqual([
        expectedPath("b", "a", "b"),
        expectedPath("c", "b", "c"),
        expectedPath("c", "a", "b", "c"),
      ]);
      expect(yield* run([edge("d", "c")])).toEqual([
        expectedPath("d", "c", "d"),
        expectedPath("d", "b", "c", "d"),
        expectedPath("d", "a", "b", "c", "d"),
      ]);
      expect(yield* run([edge("d", "b")])).toEqual([
        removedPath("d", "c"),
        expectedPath("d", "b", "d"),
        expectedPath("d", "a", "b", "d"),
      ]);
      expect(yield* run([del("nodes", "d")])).toEqual([
        removedPath("d", "b"),
        removedPath("d", "a"),
      ]);
      expect(yield* run([edge("b", "a", "B")])).toEqual([
        expectedPath("b", "a", "B"),
        expectedPath("c", "a", "B", "c"),
      ]);
      expect(yield* run([up("nodes", "b", { id: "b", parent: "a", part: "B", other: 1 })])).toEqual(
        [],
      );
      expect(yield* run([edge("b", "z", "B")])).toEqual([
        removedPath("b", "a"),
        removedPath("c", "a"),
        expectedPath("b", "z", "B"),
        expectedPath("c", "z", "B", "c"),
      ]);
      expect(yield* run([del("nodes", "b")])).toEqual([
        removedPath("b", "z"),
        removedPath("c", "z"),
      ]);
      expect(yield* run([edge("b", "z", "B")])).toEqual([
        expectedPath("b", "z", "B"),
        expectedPath("c", "z", "B", "c"),
      ]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("closure stops at self parents, cycles and the depth limit", () => {
  const { part: _part, ...withoutPart } = closureNode(64);

  const cases = [
    {
      changes: [
        edge("a", "b"),
        edge("b", "a"),
        up("nodes", "self", { id: "self", parent: "self" }),
      ],
      paths: [expectedPath("a", "b", "a"), expectedPath("b", "a", "b")],
    },
    {
      changes: [edge("b", "c"), edge("c", "a"), edge("tail", "c")],
      paths: [
        expectedPath("a", "b", "a"),
        expectedPath("a", "c", "b", "a"),
        expectedPath("b", "c", "b"),
        expectedPath("b", "a", "c", "b"),
        expectedPath("c", "a", "c"),
        expectedPath("c", "b", "a", "c"),
        expectedPath("tail", "c", "tail"),
        expectedPath("tail", "a", "c", "tail"),
        expectedPath("tail", "b", "a", "c", "tail"),
      ],
    },
    {
      changes: [up("nodes", "a", { id: "a" })],
      paths: [
        expectedPath("b", "c", "b"),
        expectedPath("b", "a", "c", "b"),
        expectedPath("c", "a", "c"),
        expectedPath("tail", "c", "tail"),
        expectedPath("tail", "a", "c", "tail"),
      ],
    },
  ];

  return Effect.gen(function* () {
    yield* migrate();

    const engine = yield* makeEngine({
      lineage: lineagePlan(closureNode(3)),
    });

    const current = new Map<string, string>();

    for (const { changes, paths } of cases) {
      materialize(current, yield* engine.process("cycle", changes));
      expect(current).toEqual(
        new Map(
          Arr.map(paths, ({ key, value }) => [identity("lineage", key), canonicalJson(value)]),
        ),
      );
    }

    for (const maxDepth of [3, 64]) {
      const chain = yield* makeEngine({
        lineage: lineagePlan({ ...withoutPart, maxDepth }),
      });

      const changes = Arr.makeBy(maxDepth + 2, (i) => edge(String(i + 1), String(i)));
      const last = String(maxDepth + 2);

      const rows = Arr.filter(
        yield* chain.process(`chain-${maxDepth}`, changes),
        (change) => change.kind === "upsert" && change.value.node === last,
      );

      expect(rows).toHaveLength(maxDepth);
      expect(rows.at(-1)).toMatchObject({ value: { node: last, ancestor: "2", depth: maxDepth } });
      expect(rows.at(-1)).not.toHaveProperty("value.path");
    }
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
});

it.live("closure rolls back an absent path part and a subtree beyond maxAffectedKeys", () =>
  Effect.gen(function* () {
    yield* migrate();
    const sql = yield* SqlClient.SqlClient;

    const lineage = plan("lineage", [
      source("nodes"),
      closureNode(3),
      { kind: "top-n", id: "all", input: "closure", orderBy: [View.key.asc()], limit: 100 },
    ]);

    const engine = yield* makeEngine({ lineage }, false, 8);
    const run = (changes: readonly StateChange[]) => engine.process("bounded", changes);

    const readState = Effect.gen(function* () {
      return [
        yield* sql`SELECT * FROM streamsy_view_integrals ORDER BY relation, row_key`,
        yield* sql`SELECT * FROM streamsy_view_arrangements ORDER BY arrangement, index_key, row_key`,
      ];
    });

    for (let i = 0; i < 9; i++) yield* run([edge(`child-${i}`, "root")]);
    const before = yield* readState;

    const absent = yield* run([up("nodes", "child-0", { id: "child-0", parent: "root" })]).pipe(
      Effect.flip,
    );

    expect(absent).toBeInstanceOf(ViewDataFault);
    expect(absent).toMatchObject({
      node: "closure",
      key: "child-0",
      reason: "closure path part is absent",
    });
    expect(yield* readState).toEqual(before);

    const overflow = yield* run([edge("root", "outside")]).pipe(Effect.flip);
    expect(overflow).toBeInstanceOf(ViewDataFault);
    expect(overflow).toMatchObject({
      node: "closure",
      key: "root",
      reason: expect.stringContaining("maxAffectedKeys (8)"),
    });
    expect(yield* readState).toEqual(before);
    expect(yield* run([edge("child-0", "other")])).toHaveLength(2);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.live("concat preserves same-key rows from both sides, including composite keys", () => {
  const p = plan("concat", [
    source("left"),
    source("right"),
    { kind: "key", id: "leftKey", input: "left", key: keyExpression(["id", "group"]) },
    { kind: "key", id: "rightKey", input: "right", key: keyExpression(["id", "group"]) },
    { kind: "concat", id: "concat", left: "leftKey", right: "rightKey" },
  ]);

  return Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine({ concat: p });
    const left = { id: "same", group: "g", value: 1 };
    const right = { id: "same", group: "g", value: 2 };
    const inputKey = encodeRowKey(["same", "g"]);
    expect(
      yield* engine.process("concat", [up("left", left.id, left), up("right", right.id, right)]),
    ).toEqual([
      { view: "concat", kind: "upsert", key: ["l", inputKey], value: left },
      { view: "concat", kind: "upsert", key: ["r", inputKey], value: right },
    ]);
    const updated = { ...right, value: 3 };
    expect(
      yield* engine.process("concat", [del("left", left.id), up("right", right.id, updated)]),
    ).toEqual([
      { view: "concat", kind: "delete", key: ["l", inputKey] },
      { view: "concat", kind: "upsert", key: ["r", inputKey], value: updated },
    ]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
});

it.live("orders mixed View.list values the same way as the full recompute", () => {
  const Item = Schema.Struct({
    id: Schema.String,
    value: Schema.Union([Schema.Finite, Schema.String]),
  });

  class Items extends Streamsy.state<Items>()("ViewsTest/MixedList", "mixed-list", {
    params: {},
    collections: { item: { schema: Item, key: "id" } },
  }) {}

  const items = View.from(Items, "item");

  const grouped = items
    .groupBy(() => ({ group: View.literal("all") }))
    .aggregate((row) => ({ values: View.list(row.value) }));

  const list = compilePlan("list", grouped.expression);
  const sourceId = JSON.stringify([Items.key, "item"]);
  const values = ["2", 10, "\ue000", -3.5, 2, "😀", "10", -3.5];

  const rows = Arr.map(values, (value, index) => {
    const id = String(index);

    return { key: id, row: { id, value } };
  });

  return Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine({ list });
    const output = new Map<string, string>();
    const changes = Arr.map(rows, ({ key, row }) => up(sourceId, key, row));
    materialize(output, yield* engine.process("mixed-list", changes));

    const expected = fullRecompute({ plan: list, sources: { [sourceId]: rows } });
    expect(Arr.map(expected.rows, ({ row }) => row.values)).toEqual([
      [-3.5, -3.5, 2, 10, "10", "2", "😀", "\ue000"],
    ]);
    expect(output).toEqual(
      new Map(
        Arr.map(expected.rows, (entry) => [identity("list", entry.key), canonicalJson(entry.row)]),
      ),
    );
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
});

it.live(
  "normalizes repeated keys, suppresses net no-ops, and restores using SQL after recompiling",
  () => {
    return Effect.gen(function* () {
      yield* migrate();
      const plans = shopPlans(true);
      const a = yield* makeEngine(plans);
      yield* a.process("one", [order("a"), customer("c")]);
      expect(
        yield* a.process("one", [order("a", "x", 100), del("orders", "a"), order("a")]),
      ).toEqual([]);
      expect(
        yield* a.process("one", [
          del("orders", "absent"),
          order("transient"),
          del("orders", "transient"),
        ]),
      ).toEqual([]);
      const b = yield* makeEngine(plans);
      expect(yield* b.process("one", [order("a"), customer("c")])).toEqual([]);
      expect((yield* b.process("two", [order("a"), customer("c")])).length).toBeGreaterThan(0);
      expect(yield* b.process("one", [])).toEqual([]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "accepts the existing typed builder, mapping/filtering rows and rekeying moves atomically",
  () => {
    const Item = Schema.Struct({ id: Schema.String, groupId: Schema.String, score: Schema.Finite });

    class Items extends Streamsy.state<Items>()("ViewsTest/Items", "items", {
      params: {},
      collections: { item: { schema: Item, key: "id" } },
    }) {}

    const items = View.from(Items, "item");

    const selected = items
      .where((row) => row.score.gt(0))
      .select((row) => ({ id: row.id, groupId: row.groupId, score: row.score }));

    const chosen = compilePlan("chosen", {
      kind: "key",
      input: selected.expression,
      key: keyExpression("groupId"),
    });

    const sourceId = JSON.stringify([Items.key, "item"]);

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ chosen });

      const first = yield* engine.process("p", [
        up(sourceId, "a", { id: "a", groupId: "g1", score: 2 }),
      ]);

      expect(first).toEqual([
        { view: "chosen", kind: "upsert", key: "g1", value: { id: "a", groupId: "g1", score: 2 } },
      ]);

      const move = yield* engine.process("p", [
        up(sourceId, "a", { id: "a", groupId: "g2", score: 2 }),
      ]);

      expect(move).toEqual([
        { view: "chosen", kind: "delete", key: "g1" },
        { view: "chosen", kind: "upsert", key: "g2", value: { id: "a", groupId: "g2", score: 2 } },
      ]);
      expect(
        yield* engine.process("p", [up(sourceId, "a", { id: "a", groupId: "g2", score: -1 })]),
      ).toEqual([{ view: "chosen", kind: "delete", key: "g2" }]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "maintains many-to-many joins, unmatched left rows, and simultaneous changes on both sides",
  () => {
    const left = source("l"),
      right = source("r");

    const joined: RelationNode = {
      kind: "left-join",

      id: "join",
      left: "l",
      right: "r",
      rightAlias: "right",
      on: {
        kind: "binary",
        operator: "equal",
        left: ref("left", "group"),
        right: ref("right", "group"),
      },
    };

    const p = plan("join", [left, right, joined]);

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ result: p });
      const current = new Map<string, string>();

      const l = new Map<string, JsonObject>(),
        r = new Map<string, JsonObject>();

      const units = [
        [up("l", "a", { id: "a", group: "g" }), up("l", "b", { id: "b", group: "g" })],
        [up("r", "x", { id: "x", group: "g" }), up("r", "y", { id: "y", group: "g" })],
        [up("l", "a", { id: "a", group: "h" }), up("r", "x", { id: "x", group: "h" })],
        [del("r", "x"), del("r", "y")],
        [del("l", "a"), del("l", "b")],
      ];

      for (const unit of units) {
        for (const c of unit) {
          const rows = c.sourceId === "l" ? l : r;

          if (c.kind === "delete") rows.delete(c.key.toString());
          else rows.set(c.key.toString(), c.value);
        }

        materialize(current, yield* engine.process("p", unit));

        const expected = fullRecompute({
          plan: p,
          sources: {
            l: Arr.map([...l], ([key, row]) => ({ key, row })),
            r: Arr.map([...r], ([key, row]) => ({ key, row })),
          },
        });

        expect(current).toEqual(
          new Map(Arr.map(expected.rows, (e) => [identity("result", e.key), canonicalJson(e.row)])),
        );
      }
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "retracts tied extrema, empty groups, and negative sums; handles missing aggregate operands",
  () => {
    const p = plan("aggregate", [
      source("items"),
      {
        kind: "grouped-aggregate",
        id: "a",
        input: "items",

        groupBy: { group: ref("row", "group") },
        aggregates: {
          count: { kind: "aggregate", function: "count" },
          sum: { kind: "aggregate", function: "sum", expression: ref("row", "v") },
          min: { kind: "aggregate", function: "min", expression: ref("row", "v") },
          max: { kind: "aggregate", function: "max", expression: ref("row", "v") },
          list: { kind: "aggregate", function: "list", expression: ref("row", "v") },
        },
      },
    ]);

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ a: p });
      expect(engine.program.arrangements.size).toBe(2); // One for sum and one for min, max and list.

      expect(
        yield* engine.process("p", [
          aggregateItem("a", -4),
          aggregateItem("b", -4),
          aggregateItem("c", 8),
          aggregateItem("d"),
        ]),
      ).toEqual([
        {
          view: "a",
          kind: "upsert",
          key: ["g"],
          value: { group: "g", count: 4, sum: 0, min: -4, max: 8, list: [-4, -4, 8] },
        },
      ]);
      expect((yield* engine.process("p", [del("items", "a"), del("items", "c")]))[0]).toMatchObject(
        { value: { count: 2, sum: -4, min: -4, max: -4, list: [-4] } },
      );
      expect((yield* engine.process("p", [del("items", "b")]))[0]).toMatchObject({
        value: { group: "g", count: 1, sum: 0, list: [] },
      });
      expect(yield* engine.process("p", [del("items", "d")])).toEqual([
        { view: "a", kind: "delete", key: ["g"] },
      ]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live("top fills vacancies, observes mixed directions and ties, and moves partitions", () => {
  const top: RelationNode = {
    kind: "top-n",
    id: "top",

    input: "items",
    partitionBy: [ref("row", "group")],
    orderBy: [
      { expression: ref("row", "v"), direction: "descending" },
      { expression: ref("row", "id"), direction: "ascending" },
    ],
    limit: 2,
  };

  const p = plan("top", [source("items"), top]);

  return Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine({ top: p });

    const first = yield* engine.process("p", [
      rankedItem("z", 1),
      rankedItem("a", 1),
      rankedItem("b", 1),
      rankedItem("c", 0),
    ]);

    expect(new Set(Arr.map(first, (c) => c.key))).toEqual(new Set(["a", "b"]));
    expect(yield* engine.process("p", [rankedItem("c", -5)])).toEqual([]);
    const move = yield* engine.process("p", [rankedItem("a", 1, "h"), del("items", "b")]);
    expect(new Set(Arr.map(move, (c) => c.key))).toEqual(new Set(["a", "b", "z", "c"]));

    const reset = yield* engine.process("p", [
      del("items", "a"),
      del("items", "z"),
      del("items", "c"),
    ]);

    expect(Arr.filter(reset, (c) => c.kind === "upsert")).toEqual([]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
});

it.live("rolls back a failed unit, including its integrals, arrangements, and accumulators", () => {
  return Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine(shopPlans());
    yield* engine.process("p", [customer("c"), order("a")]);

    const failed = yield* Effect.exit(
      engine.process("p", [
        order("b"),
        up("orders", "a", { orderId: "a", customerId: "c", total: "bad" }),
      ]),
    );

    expect(Exit.isFailure(failed)).toBe(true);
    expect(yield* engine.process("p", [customer("c"), order("a"), del("orders", "b")])).toEqual([]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
});

it.live(
  "shares identical arrangements and stores only sources and arranged relations; every statement binds at most 100 values",
  () => {
    const plans = shopPlans(true);
    const totals = plans.totals;

    if (!totals) throw new Error("missing totals fixture");

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ ...plans, anotherTotals: totals });
      const extrema = Arr.filter(engine.program.arrangements.values(), (a) => a.optional);
      expect(extrema.length).toBe(1); // min/max and two aggregate consumers share one index
      expect(Arr.filter(engine.program.nodes, (n) => n.kind === "inner-join").length).toBe(1);
      expect(engine.program.stored.size).toBe(3); // sources and joined input to a sum
      const sizes: number[] = [];
      yield* engine
        .process("p", [
          ...Array.from({ length: 211 }, (_, i) => order(`o${i}`, `c${i % 3}`, i)),
          customer("c0"),
        ])
        .pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            sizes.push(statement.compile()[1].length);

            return Effect.succeed(statement);
          }),
        );
      expect(Math.max(...sizes)).toBeLessThanOrEqual(100);
      expect(Math.max(...sizes)).toBe(100);
      const sql = yield* SqlClient.SqlClient;

      const tables = yield* sql<{
        name: string;
      }>`SELECT name FROM sqlite_master WHERE type = 'table'`;

      expect(new Set(Arr.map(tables, (t) => t.name))).toEqual(
        new Set([
          "streamsy_view_integrals",
          "streamsy_view_arrangements",
          "streamsy_view_accumulators",
          "streamsy_view_key_owners",
        ]),
      );
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live("rejects incorrect source IDs and keys", () => {
  return Effect.gen(function* () {
    yield* migrate();
    const engine = yield* makeEngine(shopPlans());
    expect(
      Exit.isFailure(
        yield* Effect.exit(engine.process("p", [up("orders", "wrong", { orderId: "other" })])),
      ),
    ).toBe(true);
    expect(Exit.isFailure(yield* Effect.exit(engine.process("p", [del("unknown", "x")])))).toBe(
      true,
    );
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
});

it("encodes numeric extremes, negative zero, strings and descending prefixes in portable order", () => {
  const values = [
    -Number.MAX_VALUE,
    -9,
    -Number.MIN_VALUE,
    -0,
    0,
    Number.MIN_VALUE,
    9,
    Number.MAX_VALUE,
  ];

  const encoded = Arr.map(values, scalarKey);
  expect(Arr.sort(encoded, Order.String)).toEqual(encoded);
  expect(scalarKey(-0)).toBe(scalarKey(0));

  const strings = Arr.map(["A", "a", "aa", "é", "😀"], scalarKey);
  expect(Arr.sort(strings, Order.String)).toEqual(strings);
  expect(() => scalarKey(null)).toThrow();
  expect(encodeRowKey(["a", "bc"])).not.toBe(encodeRowKey(["ab", "c"]));
  expect(Rec.keys(shopPlans()).length).toBe(4);
});

it.live(
  "composes nonlinear operators over stored intermediates and shares their arrangements",
  () => {
    const filtered: RelationNode = {
      kind: "filter",
      id: "positive",

      input: "orders",
      predicate: {
        kind: "binary",
        operator: "greater-than",
        left: ref("row", "total"),
        right: { kind: "literal", value: 0 },
      },
    };

    const projected: RelationNode = {
      kind: "project",
      id: "p",

      input: "positive",
      fields: {
        orderId: ref("row", "orderId"),
        customerId: ref("row", "customerId"),
        total: ref("row", "total"),
      },
    };

    const joined: RelationNode = {
      kind: "left-join",
      id: "j",

      left: "p",
      right: "customers",
      rightAlias: "customer",
      on: {
        kind: "binary",
        operator: "equal",
        left: ref("left", "customerId"),
        right: ref("right", "customerId"),
      },
    };

    const grouped: RelationNode = {
      kind: "grouped-aggregate",
      id: "g",

      input: "j",
      groupBy: {
        tierId: {
          kind: "binary",
          operator: "or-else",
          left: ref("row", "customer", "tier"),
          right: { kind: "literal", value: "absent" },
        },
      },
      aggregates: {
        sum: { kind: "aggregate", function: "sum", expression: ref("row", "total") },
        min: { kind: "aggregate", function: "min", expression: ref("row", "total") },
      },
    };

    const best: RelationNode = {
      kind: "top-n",
      id: "t",

      input: "g",
      orderBy: [
        { expression: ref("row", "sum"), direction: "descending" },
        { expression: ref("row", "tierId"), direction: "ascending" },
      ],

      limit: 2,
    };

    const prefix = [
      source("orders", "orderId"),
      source("customers", "customerId"),
      filtered,
      projected,
      joined,
      grouped,
    ];

    const plans = { best: plan("best", [...prefix, best]), groups: plan("groups", prefix) };

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine(plans);
      expect(engine.program.stored.size).toBe(5); // sources, projected join input, joined aggregate input, grouped top input

      const rows = new Map<string, JsonObject>(),
        customers = new Map<string, JsonObject>();

      const actual = new Map<string, string>();

      for (let unit = 0; unit < 40; unit++) {
        const changes = [
          order(`o${unit % 7}`, `c${unit % 3}`, ((unit * 7) % 13) - 3),
          customer(`c${unit % 3}`, unit % 2 ? "gold" : "standard"),
        ];

        if (unit % 4 === 0) changes.push(del("customers", `c${(unit + 1) % 3}`));

        for (const c of changes) {
          const relation = c.sourceId === "orders" ? rows : customers;

          if (c.kind === "delete") relation.delete(String(c.key));
          else relation.set(String(c.key), c.value);
        }

        materialize(actual, yield* engine.process("chain", changes));
        const expected = new Map<string, string>();

        for (const [name, p] of Rec.toEntries(plans))
          for (const e of fullRecompute({
            plan: p,
            sources: {
              orders: Arr.map([...rows], ([key, row]) => ({ key, row })),
              customers: Arr.map([...customers], ([key, row]) => ({ key, row })),
            },
          }).rows)
            expected.set(identity(name, e.key), canonicalJson(e.row));
        expect(actual, `chain unit ${unit}`).toEqual(expected);
      }
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "orders Unicode strings, descending prefixes, numeric extremes and key ties independently of host locale",
  () => {
    const p = plan("strings", [
      source("items"),
      {
        kind: "top-n",
        id: "t",
        input: "items",

        orderBy: [
          { expression: ref("row", "text"), direction: "descending" },
          { expression: ref("row", "id"), direction: "ascending" },
        ],
        limit: 3,
      },
    ]);

    const strings = ["", "A", "a", "aa", "a\u0000", "é", "😀", "😁", "😀x"];

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ strings: p });
      const current = new Map<string, string>();
      const rows = Arr.map(strings, (text, i) => ({ id: `s${i}`, text }));
      materialize(
        current,
        yield* engine.process(
          "p",
          Arr.map(rows, (row) => up("items", row.id, row)),
        ),
      );

      const expected = fullRecompute({
        plan: p,
        sources: { items: Arr.map(rows, (row) => ({ key: row.id, row })) },
      });

      expect(current).toEqual(
        new Map(Arr.map(expected.rows, (e) => [identity("strings", e.key), canonicalJson(e.row)])),
      );
      materialize(
        current,
        yield* engine.process("p", [del("items", "s8"), del("items", "s6"), del("items", "s7")]),
      );
      expect(new Set(current.keys())).toEqual(
        new Set(Arr.map(["s3", "s4", "s5"], (key) => identity("strings", key))),
      );

      const numbers = [
        -Number.MAX_VALUE,
        -1000,
        -1,
        -Number.MIN_VALUE,
        0,
        Number.MIN_VALUE,
        1,
        1000,
        Number.MAX_VALUE,
      ];

      for (let i = 0; i < numbers.length - 1; i++)
        expect(scalarKey(numbers[i] ?? 0) < scalarKey(numbers[i + 1] ?? 0)).toBe(true);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "shares a matching top/extrema arrangement and rejects conflicting source declarations",
  () => {
    const items = source("items");

    const group: RelationNode = {
      kind: "grouped-aggregate",
      id: "g",
      input: "items",

      groupBy: { group: ref("row", "group") },
      aggregates: { minId: { kind: "aggregate", function: "min", expression: ref("row", "id") } },
    };

    const top: RelationNode = {
      kind: "top-n",
      id: "t",
      input: "items",

      partitionBy: [ref("row", "group")],
      orderBy: [{ expression: ref("row", "id"), direction: "ascending" }],

      limit: 1,
    };

    return Effect.gen(function* () {
      yield* migrate();

      const engine = yield* makeEngine({
        grouped: plan("g", [items, group]),
        top: plan("t", [items, top]),
      });

      expect(engine.program.arrangements.size).toBe(1);

      const result = yield* engine.process("p", [
        up("items", "b", { id: "b", group: "g" }),
        up("items", "a", { id: "a", group: "g" }),
      ]);

      expect(result).toContainEqual({
        view: "grouped",
        kind: "upsert",
        key: ["g"],
        value: { group: "g", minId: "a" },
      });
      expect(result).toContainEqual({
        view: "top",
        kind: "upsert",
        key: "a",
        value: { id: "a", group: "g" },
      });

      const conflict = yield* Effect.exit(
        makeEngine({ a: plan("a", [items]), b: plan("b", [source("items", "anotherId")]) }),
      );

      expect(Exit.isFailure(conflict)).toBe(true);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "normalization reads the input integral once per distinct key in a multi-change unit",
  () => {
    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ raw: plan("raw", [source("items")]) });
      let reads = 0;

      const result = yield* engine
        .process("p", [
          up("items", "a", { id: "a", n: 1 }),
          del("items", "a"),
          up("items", "a", { id: "a", n: 2 }),
        ])
        .pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            if (statement.compile()[0].includes("SELECT value FROM streamsy_view_integrals"))
              reads++;

            return Effect.succeed(statement);
          }),
        );

      expect(reads).toBe(1);
      expect(result).toEqual([{ view: "raw", kind: "upsert", key: "a", value: { id: "a", n: 2 } }]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);

it.live(
  "uses UTF-16 string order for predicates, ordered aggregates and top, including surrogate pairs",
  () => {
    const items = source("items");

    const plans = {
      filtered: plan("filtered", [
        items,
        {
          kind: "filter",
          id: "filtered",

          input: "items",
          predicate: {
            kind: "binary",
            operator: "less-than",
            left: ref("row", "label"),
            right: { kind: "literal", value: "a" },
          },
        },
      ]),
      extrema: plan("extrema", [
        items,
        {
          kind: "grouped-aggregate",
          id: "extrema",

          input: "items",
          groupBy: { group: { kind: "literal", value: "all" } },
          aggregates: {
            first: { kind: "aggregate", function: "min", expression: ref("row", "label") },
            last: { kind: "aggregate", function: "max", expression: ref("row", "label") },
            labels: { kind: "aggregate", function: "list", expression: ref("row", "label") },
          },
        },
      ]),
      top: plan("top", [
        items,
        {
          kind: "top-n",
          id: "top",

          input: "items",

          limit: 2,
          orderBy: [
            { expression: ref("row", "label"), direction: "ascending" },
            { expression: ref("row", "id"), direction: "ascending" },
          ],
        },
      ]),
    };

    const values = ["Z", "a", "\ue000", "😀", "ä"];
    const changes = Arr.map(values, (label, i) => up("items", String(i), { id: String(i), label }));

    return Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine(plans);
      const output = yield* engine.process("strings", changes);
      expect(Arr.filter(output, (change) => change.view === "filtered")).toEqual([
        { view: "filtered", kind: "upsert", key: "0", value: { id: "0", label: "Z" } },
      ]);
      expect(Arr.filter(output, (change) => change.view === "extrema")).toEqual([
        {
          view: "extrema",
          kind: "upsert",
          key: ["all"],
          value: {
            group: "all",
            first: "Z",
            last: "\ue000",
            labels: ["Z", "a", "ä", "😀", "\ue000"],
          },
        },
      ]);
      expect(
        new Set(
          Arr.map(
            Arr.filter(output, (change) => change.view === "top"),
            (change) => change.key,
          ),
        ),
      ).toEqual(new Set(["0", "1"]));

      for (const [name, p] of Rec.toEntries(plans)) {
        const expected = fullRecompute({
          plan: p,
          sources: {
            items: Arr.map(values, (label, i) => ({
              key: String(i),
              row: { id: String(i), label },
            })),
          },
        });

        expect(Arr.filter(output, (change) => change.view === name).length).toBe(
          expected.rows.length,
        );
      }
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);
