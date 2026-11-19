import { expect, it } from "bun:test";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array as Arr, Effect, Exit, Record as Rec, Schema } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { from, source as declareSource, view } from "../../src/relation.ts";
import { literal, selectors } from "../../src/expression.ts";
import { makeEngine, migrate, sqlPlanIssues, type StateChange } from "../../src/sql/engine.ts";
import type { JsonObject, SqlRelationNode } from "../../src/ir/contracts.ts";
import { compareOrdered, scalarKey } from "../../src/sql/encoding.ts";
import { canonicalJson, encodeRowKey } from "../../src/runtime/key.ts";
import { fullRecompute } from "../../src/sql/reference.ts";
import { identity, materialize } from "./support.ts";
import { plan, ref, schema, shopPlans, source } from "./fixtures.ts";

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

it("normalizes repeated keys, suppresses net no-ops, and restores using SQL after recompiling", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("accepts the existing typed builder, mapping/filtering rows and rekeying moves atomically", async () => {
  const Item = Schema.Struct({ id: Schema.String, groupId: Schema.String, score: Schema.Finite });

  const items = declareSource("items", {
    schema: Item,
    schemaRef: schema,
    key: "id",
    mode: "state",
    partitionBy: literal("all"),
  });

  const x = selectors<typeof Item.Type>();

  const selected = view(
    "chosen",
    { schema: Item, schemaRef: schema, key: "groupId" },
    from(items)
      .where(x.row.score.gt(0))
      .select({ id: x.row.id, groupId: x.row.groupId, score: x.row.score }),
  );

  await Effect.runPromise(
    Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ chosen: selected.plan });

      const first = yield* engine.process("p", [
        up("items", "a", { id: "a", groupId: "g1", score: 2 }),
      ]);

      expect(first).toEqual([
        { view: "chosen", kind: "upsert", key: "g1", value: { id: "a", groupId: "g1", score: 2 } },
      ]);

      const move = yield* engine.process("p", [
        up("items", "a", { id: "a", groupId: "g2", score: 2 }),
      ]);

      expect(move).toEqual([
        { view: "chosen", kind: "delete", key: "g1" },
        { view: "chosen", kind: "upsert", key: "g2", value: { id: "a", groupId: "g2", score: 2 } },
      ]);
      expect(
        yield* engine.process("p", [up("items", "a", { id: "a", groupId: "g2", score: -1 })]),
      ).toEqual([{ view: "chosen", kind: "delete", key: "g2" }]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("maintains many-to-many joins, unmatched left rows, and simultaneous changes on both sides", async () => {
  const left = source("l"),
    right = source("r");

  const joined: SqlRelationNode = {
    kind: "left-join",
    schema,
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
  await Effect.runPromise(
    Effect.gen(function* () {
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("retracts tied extrema, empty groups, and negative sums; handles missing aggregate operands", async () => {
  const p = plan("aggregate", [
    source("items"),
    {
      kind: "grouped-aggregate",
      id: "a",
      input: "items",
      schema,
      groupBy: { group: ref("row", "group") },
      aggregates: {
        count: { kind: "aggregate", function: "count" },
        sum: { kind: "aggregate", function: "sum", expression: ref("row", "v") },
        min: { kind: "aggregate", function: "min", expression: ref("row", "v") },
        max: { kind: "aggregate", function: "max", expression: ref("row", "v") },
      },
    },
  ]);

  await Effect.runPromise(
    Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ a: p });

      const put = (id: string, v?: number) =>
        up("items", id, v === undefined ? { id, group: "g" } : { id, group: "g", v });

      expect(
        yield* engine.process("p", [put("a", -4), put("b", -4), put("c", 8), put("d")]),
      ).toEqual([
        {
          view: "a",
          kind: "upsert",
          key: ["g"],
          value: { group: "g", count: 4, sum: 0, min: -4, max: 8 },
        },
      ]);
      expect((yield* engine.process("p", [del("items", "a"), del("items", "c")]))[0]).toMatchObject(
        { value: { count: 2, sum: -4, min: -4, max: -4 } },
      );
      expect((yield* engine.process("p", [del("items", "b")]))[0]).toMatchObject({
        value: { group: "g", count: 1, sum: 0 },
      });
      expect(yield* engine.process("p", [del("items", "d")])).toEqual([
        { view: "a", kind: "delete", key: ["g"] },
      ]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("top fills vacancies, observes mixed directions and ties, and moves partitions", async () => {
  const top: SqlRelationNode = {
    kind: "top-n",
    id: "top",
    schema,
    input: "items",
    partitionBy: [ref("row", "group")],
    orderBy: [
      { expression: ref("row", "v"), direction: "descending" },
      { expression: ref("row", "id"), direction: "ascending" },
    ],
    limit: { kind: "literal", value: 2 },
    maximum: 2,
  };

  const p = plan("top", [source("items"), top]);
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ top: p });
      const put = (id: string, v: number, group = "g") => up("items", id, { id, v, group });

      const first = yield* engine.process("p", [
        put("z", 1),
        put("a", 1),
        put("b", 1),
        put("c", 0),
      ]);

      expect(new Set(Arr.map(first, (c) => c.key))).toEqual(new Set(["a", "b"]));
      expect(yield* engine.process("p", [put("c", -5)])).toEqual([]);
      const move = yield* engine.process("p", [put("a", 1, "h"), del("items", "b")]);
      expect(new Set(Arr.map(move, (c) => c.key))).toEqual(new Set(["a", "b", "z", "c"]));

      const reset = yield* engine.process("p", [
        del("items", "a"),
        del("items", "z"),
        del("items", "c"),
      ]);

      expect(Arr.filter(reset, (c) => c.kind === "upsert")).toEqual([]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("rolls back a failed unit, including its integrals, arrangements, and accumulators", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
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
      expect(yield* engine.process("p", [customer("c"), order("a"), del("orders", "b")])).toEqual(
        [],
      );
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("shares identical arrangements and stores only nonlinear inputs; every statement binds at most 100 values", async () => {
  const plans = shopPlans(true);
  const totals = plans.totals;

  if (!totals) throw new Error("missing totals fixture");
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* migrate();
      const engine = yield* makeEngine({ ...plans, anotherTotals: totals });
      const extrema = Arr.filter(engine.program.arrangements, (a) => a.optional);
      expect(extrema.length).toBe(1); // min/max and two aggregate consumers share one index
      expect(Arr.filter(engine.program.nodes, (n) => n.kind === "inner-join").length).toBe(1);
      expect(engine.program.stored.size).toBe(2); // only the two sources
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
        ]),
      );
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("checks unsupported plans, invalid top orders, cycles, facts, and input key mismatches", async () => {
  const fact = source("facts");

  if (fact.kind !== "source") throw new Error("source fixture");

  const bad = plan("bad", [
    { ...fact, mode: "facts" },
    {
      kind: "reduce-by-key",
      id: "fold",
      schema,
      input: "facts",
      key: ref("row", "id"),
      reducer: schema,
    },
  ]);

  expect(
    Arr.join(
      Arr.map(sqlPlanIssues(bad), (e) => e.message),
      " ",
    ),
  ).toContain("fact folds");

  const badJoin = plan("badJoin", [
    source("l"),
    source("r"),
    {
      kind: "inner-join",
      id: "j",
      schema,
      left: "l",
      right: "r",
      rightAlias: "right",
      on: { kind: "literal", value: true },
    },
  ]);

  expect(sqlPlanIssues(badJoin).length).toBeGreaterThan(0);

  const cycle = plan("cycle", [
    { kind: "filter", id: "f", input: "f", schema, predicate: { kind: "literal", value: true } },
  ]);

  expect(sqlPlanIssues(cycle).length).toBeGreaterThan(0);
  await Effect.runPromise(
    Effect.gen(function* () {
      expect(Exit.isFailure(yield* Effect.exit(makeEngine({ bad })))).toBe(true);
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
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

  for (let i = 0; i < values.length - 1; i++) {
    expect(compareOrdered(values[i] ?? 0, values[i + 1] ?? 0)).toBeLessThanOrEqual(0);
  }

  expect(scalarKey(-0)).toBe(scalarKey(0));
  expect(compareOrdered("a", "aa")).toBe(-1);
  expect(compareOrdered("A", "a")).toBe(-1);
  expect(compareOrdered("é", "😀")).toBe(-1);
  expect(() => scalarKey(null)).toThrow();
  expect(encodeRowKey(["a", "bc"])).not.toBe(encodeRowKey(["ab", "c"]));
  expect(Rec.keys(shopPlans()).length).toBe(4);
});

it("composes nonlinear operators over stored intermediates and shares their arrangements", async () => {
  const filtered: SqlRelationNode = {
    kind: "filter",
    id: "positive",
    schema,
    input: "orders",
    predicate: {
      kind: "binary",
      operator: "greater-than",
      left: ref("row", "total"),
      right: { kind: "literal", value: 0 },
    },
  };

  const projected: SqlRelationNode = {
    kind: "project",
    id: "p",
    schema,
    input: "positive",
    fields: {
      orderId: ref("row", "orderId"),
      customerId: ref("row", "customerId"),
      total: ref("row", "total"),
    },
  };

  const joined: SqlRelationNode = {
    kind: "left-join",
    id: "j",
    schema,
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

  const grouped: SqlRelationNode = {
    kind: "grouped-aggregate",
    id: "g",
    schema,
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

  const best: SqlRelationNode = {
    kind: "top-n",
    id: "t",
    schema,
    input: "g",
    orderBy: [
      { expression: ref("row", "sum"), direction: "descending" },
      { expression: ref("row", "tierId"), direction: "ascending" },
    ],
    maximum: 2,
    limit: { kind: "literal", value: 2 },
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
  await Effect.runPromise(
    Effect.gen(function* () {
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("orders Unicode strings, descending prefixes, numeric extremes and key ties independently of host locale", async () => {
  const p = plan("strings", [
    source("items"),
    {
      kind: "top-n",
      id: "t",
      input: "items",
      schema,
      orderBy: [
        { expression: ref("row", "text"), direction: "descending" },
        { expression: ref("row", "id"), direction: "ascending" },
      ],
      limit: { kind: "literal", value: 3 },
      maximum: 3,
    },
  ]);

  const strings = ["", "A", "a", "aa", "a\u0000", "é", "😀", "😁", "😀x"];
  await Effect.runPromise(
    Effect.gen(function* () {
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
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
});

it("shares a matching top/extrema arrangement and rejects conflicting source declarations", async () => {
  const items = source("items");

  const group: SqlRelationNode = {
    kind: "grouped-aggregate",
    id: "g",
    input: "items",
    schema,
    groupBy: { group: ref("row", "group") },
    aggregates: { minId: { kind: "aggregate", function: "min", expression: ref("row", "id") } },
  };

  const top: SqlRelationNode = {
    kind: "top-n",
    id: "t",
    input: "items",
    schema,
    partitionBy: [ref("row", "group")],
    orderBy: [{ expression: ref("row", "id"), direction: "ascending" }],
    maximum: 1,
    limit: { kind: "literal", value: 1 },
  };

  await Effect.runPromise(
    Effect.gen(function* () {
      yield* migrate();

      const engine = yield* makeEngine({
        grouped: plan("g", [items, group]),
        top: plan("t", [items, top]),
      });

      expect(engine.program.arrangements.length).toBe(1);

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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});

it("normalization reads the input integral once per distinct key in a multi-change unit", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
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
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  );
});
