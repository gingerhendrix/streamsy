import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array as Arr, Effect } from "effect";
import { compareRowKeys, encodeRowKey } from "../../src/runtime/key.ts";
import { makeEngine } from "../../src/sql/engine.ts";
import { runtime } from "../../src/sql/runtime.ts";
import { fullRecompute } from "../support/full-recompute.ts";
import { plan, ref, source } from "./fixtures.ts";
import { migratedSqliteBackend } from "./support.ts";

const sqlite = SqliteClient.layer({ filename: ":memory:" });

const bmp = "\ue000\ue000";

const astral = "😀";

const astralRow = { id: astral, rankId: 1 };

const items = [
  { key: astral, row: astralRow },
  { key: bmp, row: { id: bmp, rankId: 1 } },
];

const input = Arr.map(items, ({ key, row }) => ({
  sourceId: "items",
  kind: "upsert" as const,
  key,
  value: row,
}));

const top = (limit: number, expression = ref("row", "rankId")) =>
  plan("top", [
    source("items"),
    {
      kind: "top-n",
      id: "top",
      input: "items",
      limit,
      orderBy: [{ expression, direction: "ascending" }],
    },
  ]);

it.live("orders tied astral and BMP top rows in the same unit", () =>
  Effect.gen(function* () {
    const backend = yield* migratedSqliteBackend;
    const engine = yield* makeEngine({ top: top(2) });
    const changes = yield* engine.process(backend, "rows", input);
    expect(Arr.map(changes, ({ key }) => key)).toEqual([bmp, astral]);
  }).pipe(Effect.provide(sqlite)),
);

it.live("suppresses a top delta when the next unit removes the losing astral tie", () =>
  Effect.gen(function* () {
    const backend = yield* migratedSqliteBackend;
    const engine = yield* makeEngine({ top: top(1) });
    expect(Arr.map(yield* engine.process(backend, "rows", input), ({ key }) => key)).toEqual([bmp]);
    expect(
      yield* engine.process(backend, "rows", [{ sourceId: "items", kind: "delete", key: astral }]),
    ).toEqual([]);
  }).pipe(Effect.provide(sqlite)),
);

it.live("replaces the top winner when the next unit removes the winning BMP tie", () =>
  Effect.gen(function* () {
    const backend = yield* migratedSqliteBackend;
    const engine = yield* makeEngine({ top: top(1) });
    expect(Arr.map(yield* engine.process(backend, "rows", input), ({ key }) => key)).toEqual([bmp]);
    expect(
      yield* engine.process(backend, "rows", [{ sourceId: "items", kind: "delete", key: bmp }]),
    ).toEqual([
      { view: "top", kind: "delete", key: bmp },
      { view: "top", kind: "upsert", key: astral, value: { id: astral, rankId: 1 } },
    ]);
  }).pipe(Effect.provide(sqlite)),
);

it.live("restores the reverse row-key order in a next-unit before image", () =>
  Effect.gen(function* () {
    const backend = yield* migratedSqliteBackend;
    const engine = yield* makeEngine({ top: top(2) });
    const [arrangement] = engine.program.arrangements.values();

    if (arrangement === undefined) throw new Error("Missing top arrangement");

    yield* engine.process(backend, "rows", input);
    const store = backend.store("rows");
    const rt = runtime(engine.program, store, {});

    yield* rt.apply(
      arrangement.relation,
      new Map([
        [
          encodeRowKey(astral),
          {
            key: astral,
            before: astralRow,
            after: undefined,
          },
        ],
      ]),
    );
    const previous = yield* rt.lookup(arrangement, "[]", true, { limit: 1, reverse: true });
    expect(Arr.map(previous, ({ key }) => key)).toEqual([astral]);
  }).pipe(Effect.provide(sqlite)),
);

it.live("uses the same row-key ties in the oracle and preserves explicit key sort terms", () =>
  Effect.gen(function* () {
    const backend = yield* migratedSqliteBackend;
    const keyed = top(2, ref("key"));
    const engine = yield* makeEngine({ top: keyed });
    expect(Arr.map(yield* engine.process(backend, "rows", input), ({ key }) => key)).toEqual([
      astral,
      bmp,
    ]);
    expect(fullRecompute({ plan: keyed, sources: { items } }).ordered).toEqual([
      { partition: [], keys: [astral, bmp] },
    ]);
    expect(fullRecompute({ plan: top(2), sources: { items } }).ordered).toEqual([
      { partition: [], keys: [bmp, astral] },
    ]);
  }).pipe(Effect.provide(sqlite)),
);

it.live("sums floating-point operands in encoded row-key order", () =>
  Effect.gen(function* () {
    const backend = yield* migratedSqliteBackend;

    const grouped = plan("sum", [
      source("items"),
      {
        kind: "grouped-aggregate",
        id: "sum",
        input: "items",
        groupBy: { group: { kind: "literal", value: "all" } },
        aggregates: {
          total: { kind: "aggregate", function: "sum", expression: ref("row", "value") },
        },
      },
    ]);

    const engine = yield* makeEngine({ sum: grouped });

    const rows = [
      { key: "aa", row: { id: "aa", value: 1e16 } },
      { key: bmp, row: { id: bmp, value: -1e16 } },
      { key: astral, row: { id: astral, value: 1 } },
    ];

    const changes = yield* engine.process(
      backend,
      "rows",
      Arr.map(rows, ({ key, row }) => ({
        sourceId: "items",
        kind: "upsert" as const,
        key,
        value: row,
      })),
    );

    expect(changes).toEqual([
      { view: "sum", kind: "upsert", key: ["all"], value: { group: "all", total: 1 } },
    ]);
    expect(fullRecompute({ plan: grouped, sources: { items: rows } }).rows).toEqual([
      { key: ["all"], row: { group: "all", total: 1 } },
    ]);
  }).pipe(Effect.provide(sqlite)),
);

it("orders lone surrogates as replacement characters", () => {
  expect(compareRowKeys("s1:\ud800", "s1:\ufffd")).toBe(0);
  expect(compareRowKeys("s1:\udfff", "s1:\ufffd")).toBe(0);
  expect(compareRowKeys("s1:\ue000", "s1:\ud800")).toBeLessThan(0);
  expect(compareRowKeys("s2:\ud800a", "s2:\ufffdb")).toBeLessThan(0);
  expect(compareRowKeys("s2:\ud800a", "s2:😀")).toBeLessThan(0);
});
