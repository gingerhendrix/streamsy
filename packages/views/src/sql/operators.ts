import { Array as Arr, Effect, Record as Rec } from "effect";
import { evaluate, evaluateOptional, isMissing } from "../runtime/expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "../runtime/key.ts";
import type {
  JsonObject,
  JsonValue,
  SqlGroupedAggregateNode,
  SqlRelationNode,
  TopNNode,
} from "../ir/contracts.ts";
import { findArrangement, joinKeys, type Program } from "./planner.ts";
import { difference, type Deltas, type Runtime } from "./runtime.ts";
import type { Accumulator, Entry, Store } from "./store.ts";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-known-value-widening -- Mutable aggregate fields and sums use the IR's open dictionary contract; Expression results are checked at the operator boundary. */

export function linear(
  node: Extract<SqlRelationNode, { kind: "filter" | "project" | "key" }>,
  input: Deltas,
): Deltas {
  const map = (entry: Entry): Entry | undefined => {
    if (node.kind === "filter") {
      const accepted = evaluate(node.predicate, { row: entry.row, key: entry.key });

      if (typeof accepted !== "boolean") throw new TypeError("filter predicate must be Boolean");

      return accepted ? entry : undefined;
    }

    if (node.kind === "key")
      return { ...entry, key: asRowKey(evaluate(node.key, { row: entry.row, key: entry.key })) };
    const row: Record<string, JsonValue> = {};

    for (const [name, e] of Rec.toEntries(node.fields)) {
      const value = evaluateOptional(e, { row: entry.row, key: entry.key });

      if (!isMissing(value)) row[name] = value;
    }

    return { key: entry.key, row };
  };

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const d of input.values()) {
    const a = d.before === undefined ? undefined : map({ key: d.key, row: d.before });
    const b = d.after === undefined ? undefined : map({ key: d.key, row: d.after });

    if (a) before.push(a);

    if (b) after.push(b);
  }

  return difference(before, after);
}

export const join = Effect.fnUntraced(function* (
  node: Extract<SqlRelationNode, { kind: "inner-join" | "left-join" }>,
  program: Program,
  rt: Runtime,
) {
  const keys = joinKeys(node.on);

  if (!keys) throw new TypeError("join was not checked");
  const leftIndex = findArrangement(program, node.left, [keys.left], []);
  const rightIndex = findArrangement(program, node.right, [keys.right], []);
  const affected = new Set(rt.changes.get(node.left)?.keys());
  const rightKeys = new Set<string>();

  for (const d of rt.changes.get(node.right)?.values() ?? []) {
    for (const row of [d.before, d.after]) {
      if (!row) continue;
      const key = evaluateOptional(keys.right, { row, key: d.key });

      if (!isMissing(key)) rightKeys.add(canonicalJson([key]));
    }
  }

  for (const key of rightKeys)
    for (const before of [true, false]) {
      for (const entry of yield* rt.lookup(leftIndex, key, before))
        affected.add(encodeRowKey(entry.key));
    }

  const evaluateLeft = (id: string, before: boolean) =>
    Effect.gen(function* () {
      const left = yield* rt.get(node.left, id, before);

      if (!left) return [];
      const key = evaluateOptional(keys.left, { row: left.row, key: left.key });

      const right = isMissing(key)
        ? []
        : yield* rt.lookup(rightIndex, canonicalJson([key]), before);

      if (right.length === 0)
        return node.kind === "left-join"
          ? [{ key: ["unmatched", encodeRowKey(left.key)], row: left.row }]
          : [];

      return Arr.map(right, (r) => ({
        key: ["match", encodeRowKey(left.key), encodeRowKey(r.key)],
        row: { ...left.row, [node.rightAlias]: r.row },
      }));
    });

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const id of affected) {
    before.push(...(yield* evaluateLeft(id, true)));
    after.push(...(yield* evaluateLeft(id, false)));
  }

  return difference(before, after);
});

export const aggregate = Effect.fnUntraced(function* (
  node: SqlGroupedAggregateNode,
  program: Program,
  rt: Runtime,
  store: Store,
) {
  const expressions = Rec.values(node.groupBy);

  const groups = new Map<
    string,
    {
      key: ReturnType<typeof asRowKey>;
      row: JsonObject;
      count: number;
      sums: Record<string, number>;
    }
  >();

  for (const d of rt.changes.get(node.input)?.values() ?? []) {
    for (const [row, sign] of [
      [d.before, -1],
      [d.after, 1],
    ] as const) {
      if (row === undefined) continue;
      const fields: Record<string, JsonValue> = {};

      for (const [name, e] of Rec.toEntries(node.groupBy))
        fields[name] = evaluate(e, { row, key: d.key });
      const values = Rec.values(fields);

      const key = asRowKey(values),
        encoded = canonicalJson(values);

      const group = groups.get(encoded) ?? { key, row: fields, count: 0, sums: {} };
      group.count += sign;

      for (const [name, a] of Rec.toEntries(node.aggregates)) {
        if (a.function !== "sum" || !a.expression) continue;
        const value = evaluateOptional(a.expression, { row, key: d.key });

        if (isMissing(value)) continue;

        if (typeof value !== "number" || !Number.isFinite(value))
          throw new TypeError("sum requires finite numbers");
        group.sums[name] = (group.sums[name] ?? 0) + sign * value;
      }

      groups.set(encoded, group);
    }
  }

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const [encoded, g] of groups) {
    const old = yield* store.accumulator(node.id, encoded);
    const sums: Record<string, number> = { ...old?.sums };

    for (const [name, change] of Rec.toEntries(g.sums)) {
      sums[name] = (sums[name] ?? 0) + change;

      if (!Number.isFinite(sums[name])) throw new TypeError("sum overflow");
    }

    const next: Accumulator = { count: (old?.count ?? 0) + g.count, sums };

    if (next.count < 0) throw new TypeError("negative group count");

    const derive = (acc: Accumulator | undefined, prior: boolean) =>
      Effect.gen(function* () {
        if (!acc || acc.count === 0) return undefined;
        const row: Record<string, JsonValue> = { ...g.row };

        for (const [name, a] of Rec.toEntries(node.aggregates)) {
          if (a.function === "count") row[name] = acc.count;
          else if (a.function === "sum") row[name] = acc.sums[name] ?? 0;
          else if ((a.function === "min" || a.function === "max") && a.expression) {
            const arrangement = findArrangement(program, node.input, expressions, [
              { expression: a.expression, direction: "ascending" },
            ]);

            const first = (yield* rt.lookup(
              arrangement,
              encoded,
              prior,
              1,
              a.function === "max",
            ))[0];

            if (first) row[name] = evaluate(a.expression, { row: first.row, key: first.key });
          } else throw new TypeError(`unsupported aggregate ${a.function}`);
        }

        return { key: g.key, row };
      });

    const a = yield* derive(old, true);

    if (a) before.push(a);

    if (next.count === 0) yield* store.accumulatorRemove(node.id, encoded);
    else yield* store.accumulatorPut(node.id, encoded, next);
    const b = yield* derive(next, false);

    if (b) after.push(b);
  }

  return difference(before, after);
});

export const top = Effect.fnUntraced(function* (node: TopNNode, program: Program, rt: Runtime) {
  const limit = evaluate(node.limit, {});

  if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0 || limit > node.maximum)
    throw new TypeError("invalid top limit");
  const a = findArrangement(program, node.input, node.partitionBy ?? [], node.orderBy);
  const partitions = new Set<string>();

  for (const d of rt.changes.get(node.input)?.values() ?? [])
    for (const row of [d.before, d.after]) {
      if (!row) continue;
      const values = Arr.map(node.partitionBy ?? [], (e) => evaluate(e, { row, key: d.key }));
      asRowKey(values);
      partitions.add(canonicalJson(values));
    }

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const key of partitions) {
    before.push(...(yield* rt.lookup(a, key, true, limit)));
    after.push(...(yield* rt.lookup(a, key, false, limit)));
  }

  return difference(before, after);
});
