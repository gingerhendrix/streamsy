import { Array as Arr, Effect, Predicate, Record as Rec } from "effect";
import {
  evaluate,
  evaluateOptional,
  isMissing,
  type ExpressionScopes,
} from "../runtime/expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "../runtime/key.ts";
import {
  isJsonObject,
  type ClosureNode,
  type ConcatNode,
  type JsonObject,
  type JsonValue,
  type GroupedAggregateNode,
  type RelationNode,
  type TopNNode,
  type RowKey,
} from "../ir/contracts.ts";
import { checkData, type DataFault } from "./data-fault.ts";
import { findArrangement, joinKeys, type Program } from "./planner.ts";
import { difference, type Delta, type Deltas, type Runtime } from "./runtime.ts";
import type { Accumulator, Entry, Store } from "./store.ts";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-known-value-widening -- Mutable aggregate fields and sums use the IR's open dictionary contract; Expression results are checked at the operator boundary. */

export function accepts(
  node: Extract<RelationNode, { kind: "filter" }>,
  key: RowKey,
  row: JsonObject,
  fault: DataFault,
  parameter: ExpressionScopes["parameter"],
): boolean {
  return checkData(fault, key, () => {
    const accepted = evaluate(node.predicate, { row, key, parameter });

    if (typeof accepted !== "boolean") throw fault(key, "filter predicate must be Boolean");

    return accepted;
  });
}

export function linear(
  node: Extract<RelationNode, { kind: "filter" | "project" }>,
  input: Deltas,
  fault: DataFault,
  parameter: ExpressionScopes["parameter"],
): Deltas {
  const map = (entry: Entry): Entry | undefined => {
    if (node.kind === "filter")
      return accepts(node, entry.key, entry.row, fault, parameter) ? entry : undefined;

    return checkData(fault, entry.key, () => {
      const scopes = { row: entry.row, key: entry.key, parameter };

      if (node.row !== undefined) {
        const row = evaluate(node.row, scopes);

        if (!isJsonObject(row)) throw fault(entry.key, "row projection must produce a JSON object");

        return { key: entry.key, row };
      }

      const row: Record<string, JsonValue> = {};

      for (const [name, e] of Rec.toEntries(node.fields)) {
        const value = evaluateOptional(e, scopes);

        if (!isMissing(value)) row[name] = value;
      }

      return { key: entry.key, row };
    });
  };

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const d of input.values()) {
    const a = d.before === undefined ? undefined : map({ key: d.key, row: d.before });
    const b = d.after === undefined ? undefined : map({ key: d.key, row: d.after });

    if (a) before.push(a);

    if (b) after.push(b);
  }

  return difference(before, after, fault);
}

export function concat(node: ConcatNode, rt: Runtime): Deltas {
  const result = new Map<string, Delta>();

  for (const [tag, input] of [
    ["l", node.left],
    ["r", node.right],
  ] as const) {
    for (const [id, change] of rt.changes.get(input) ?? []) {
      const key = [tag, id];
      result.set(encodeRowKey(key), { ...change, key });
    }
  }

  return result;
}

export const closure = Effect.fnUntraced(function* (
  node: ClosureNode,
  program: Program,
  rt: Runtime,
) {
  const fault = rt.fault(node.id);
  const byParent = findArrangement(program, node.input, [node.parent], []);

  const lookupOptions =
    rt.fanOutReadLimit === undefined ? undefined : { limit: rt.fanOutReadLimit };

  const stringKey = (key: RowKey) => {
    if (!Predicate.isString(key)) throw fault(key, "closure requires string row keys");

    return key;
  };

  const edge = (key: string, row: JsonObject | undefined) => {
    if (row === undefined) return null;

    return checkData(fault, key, () => {
      const scopes = { row, key, parameter: rt.parameter };
      const parent = evaluateOptional(node.parent, scopes);

      if (isMissing(parent) || parent === key) return null;

      if (!Predicate.isString(parent)) throw fault(key, "closure requires string parent keys");

      if (node.part === undefined) return { parent };

      const part = evaluateOptional(node.part, scopes);

      return isMissing(part) ? { parent } : { parent, part };
    });
  };

  const dirty = new Set<string>();

  for (const change of rt.changes.get(node.input)?.values() ?? []) {
    const key = stringKey(change.key);

    if (canonicalJson(edge(key, change.before)) !== canonicalJson(edge(key, change.after))) {
      dirty.add(key);
      rt.checkFanOut?.(node.id, dirty.size, key);
    }
  }

  // A multi-source walk per image visits each node first via the closest dirty ancestor.
  const affected = new Set(dirty);

  for (const before of [true, false]) {
    const seen = new Set(dirty);
    let frontier = Arr.map([...dirty], (key) => ({ key, trigger: key }));

    for (let depth = 1; depth < node.maxDepth && frontier.length > 0; depth++) {
      const next: typeof frontier = [];

      for (const { key, trigger } of frontier) {
        const children = yield* rt.lookup(byParent, canonicalJson([key]), before, lookupOptions);

        for (const child of children) {
          const childKey = stringKey(child.key);

          if (seen.has(childKey)) continue;
          seen.add(childKey);
          affected.add(childKey);
          rt.checkFanOut?.(node.id, affected.size, trigger);
          next.push({ key: childKey, trigger });
        }
      }

      frontier = next;
    }
  }

  const ancestors = (key: string, before: boolean, output: Entry[]) =>
    Effect.gen(function* () {
      const encodedNode = encodeRowKey(key);
      const visited = new Set([key]);
      const path: JsonValue[] = [];
      let current = key;

      for (let depth = 1; depth <= node.maxDepth; depth++) {
        const entry = yield* rt.get(node.input, encodeRowKey(current), before);
        const next = edge(current, entry?.row);

        if (next === null || visited.has(next.parent)) break;

        const { parent, part } = next;
        const row = { node: key, ancestor: parent, depth };

        if (node.part !== undefined) {
          if (part === undefined) throw fault(current, "closure path part is absent");
          path.unshift(part);
        }

        output.push({
          key: [encodedNode, encodeRowKey(parent)],
          row: node.part === undefined ? row : { ...row, path: [...path] },
        });
        visited.add(parent);
        current = parent;
      }
    });

  const before: Entry[] = [];
  const after: Entry[] = [];

  for (const key of affected) {
    yield* ancestors(key, true, before);
    yield* ancestors(key, false, after);
  }

  return difference(before, after, fault);
});

export const join = Effect.fnUntraced(function* (
  node: Extract<RelationNode, { kind: "inner-join" | "left-join" }>,
  program: Program,
  rt: Runtime,
) {
  const fault = rt.fault(node.id);
  const keys = joinKeys(node.on);

  if (!keys) throw new TypeError("join was not checked");
  const leftIndex = findArrangement(program, node.left, [keys.left], []);
  const rightIndex = findArrangement(program, node.right, [keys.right], []);
  const affected = new Set(rt.changes.get(node.left)?.keys());
  const rightKeys = new Map<string, RowKey>();

  const lookupOptions =
    rt.fanOutReadLimit === undefined ? undefined : { limit: rt.fanOutReadLimit };

  for (const d of rt.changes.get(node.right)?.values() ?? []) {
    for (const row of [d.before, d.after]) {
      if (!row) continue;

      const key = checkData(fault, d.key, () =>
        evaluateOptional(keys.right, { row, key: d.key, parameter: rt.parameter }),
      );

      if (!isMissing(key)) rightKeys.set(canonicalJson([key]), d.key);
    }
  }

  for (const [key, trigger] of rightKeys)
    for (const before of [true, false]) {
      for (const entry of yield* rt.lookup(leftIndex, key, before, lookupOptions)) {
        affected.add(encodeRowKey(entry.key));
        rt.checkFanOut?.(node.id, affected.size, trigger);
      }
    }

  const matched = lookupOptions === undefined ? undefined : new Set<string>();

  const record = (entry: Entry, trigger: RowKey) => {
    if (matched !== undefined) {
      matched.add(encodeRowKey(entry.key));
      rt.checkFanOut?.(node.id, matched.size, trigger);
    }

    return entry;
  };

  const evaluateLeft = (id: string, before: boolean) =>
    Effect.gen(function* () {
      const left = yield* rt.get(node.left, id, before);

      if (!left) return [];

      const key = checkData(fault, left.key, () =>
        evaluateOptional(keys.left, { row: left.row, key: left.key, parameter: rt.parameter }),
      );

      const right = isMissing(key)
        ? []
        : yield* rt.lookup(rightIndex, canonicalJson([key]), before, lookupOptions);

      if (right.length === 0)
        return node.kind === "left-join"
          ? [record({ key: ["unmatched", encodeRowKey(left.key)], row: left.row }, left.key)]
          : [];

      return Arr.map(right, (r) =>
        record(
          {
            key: ["match", encodeRowKey(left.key), encodeRowKey(r.key)],
            row: { ...left.row, [node.rightAlias]: r.row },
          },
          left.key,
        ),
      );
    });

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const id of affected) {
    before.push(...(yield* evaluateLeft(id, true)));
    after.push(...(yield* evaluateLeft(id, false)));
  }

  return difference(before, after, fault);
});

export const aggregate = Effect.fnUntraced(function* (
  node: GroupedAggregateNode,
  program: Program,
  rt: Runtime,
  store: Store,
) {
  const fault = rt.fault(node.id);
  const expressions = Rec.values(node.groupBy);

  const sumFields = Arr.flatMap(Rec.toEntries(node.aggregates), ([name, a]) =>
    a.function === "sum" && a.expression ? [[name, a.expression] as const] : [],
  );

  const operands = (row: JsonObject, key: RowKey, sign: 1 | -1) =>
    checkData(fault, key, () =>
      Arr.flatMap(sumFields, ([field, expression]) => {
        const value = evaluateOptional(expression, { row, key, parameter: rt.parameter });

        if (isMissing(value)) return [];

        if (typeof value !== "number" || !Number.isFinite(value))
          throw fault(key, "sum requires finite numbers");

        return [{ field, value, sign }];
      }),
    );

  const groups = new Map<
    string,
    {
      key: ReturnType<typeof asRowKey>;
      row: JsonObject;
      count: number;
      operands: SignedOperand[];
    }
  >();

  for (const d of rt.changes.get(node.input)?.values() ?? []) {
    for (const [row, sign] of [
      [d.before, -1],
      [d.after, 1],
    ] as const) {
      if (row === undefined) continue;

      const { key, fields } = checkData(fault, d.key, () => {
        const groupFields = Rec.map(node.groupBy, (e) =>
          evaluate(e, { row, key: d.key, parameter: rt.parameter }),
        );

        return { key: asRowKey(Rec.values(groupFields)), fields: groupFields };
      });

      const encoded = canonicalJson(key);
      const group = groups.get(encoded) ?? { key, row: fields, count: 0, operands: [] };
      group.count += sign;
      group.operands.push(...operands(row, d.key, sign));

      groups.set(encoded, group);
      rt.checkFanOut?.(node.id, groups.size, key);
    }
  }

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const [encoded, g] of groups) {
    const old = yield* store.accumulator(node.id, encoded);
    const count = (old?.count ?? 0) + g.count;

    if (count < 0) throw new Error("negative group count");

    let next: Accumulator = { count, sums: {} };

    if (count > 0 && sumFields.length > 0) {
      const exact =
        old === undefined || old.integerMass !== undefined
          ? addIntegers(old, g.operands)
          : undefined;

      if (exact) next = { count, ...exact };
      else {
        const index = findArrangement(program, node.input, expressions, []);
        const sums: Record<string, number> = {};
        let integerMass: Record<string, number> | undefined = {};

        // The arrangement returns canonical encoded-row-key order. Rebuild from
        // live operands so deletions, replays and unit boundaries round identically.
        for (const entry of yield* rt.lookup(index, encoded, false)) {
          for (const { field, value } of operands(entry.row, entry.key, 1)) {
            sums[field] = (sums[field] ?? 0) + value;

            if (integerMass) {
              const mass = (integerMass[field] ?? 0) + Math.abs(value);

              if (Number.isSafeInteger(value) && Number.isSafeInteger(mass))
                integerMass[field] = mass;
              else integerMass = undefined;
            }
          }
        }

        if (Arr.some(Rec.values(sums), (sum) => !Number.isFinite(sum)))
          return yield* fault(g.key, "sum overflow");
        next = integerMass ? { count, sums, integerMass } : { count, sums };
      }
    }

    const derive = (acc: Accumulator | undefined, prior: boolean) =>
      Effect.gen(function* () {
        if (!acc || acc.count === 0) return undefined;
        const row: Record<string, JsonValue> = { ...g.row };

        for (const [name, a] of Rec.toEntries(node.aggregates)) {
          if (a.function === "count") row[name] = acc.count;
          else if (a.function === "sum") row[name] = acc.sums[name] ?? 0;
          else if (a.expression) {
            const expression = a.expression;

            const arrangement = findArrangement(program, node.input, expressions, [
              { expression, direction: "ascending" },
            ]);

            const read = (entry: Entry) =>
              checkData(fault, entry.key, () =>
                evaluate(expression, { row: entry.row, key: entry.key, parameter: rt.parameter }),
              );

            if (a.function === "list") {
              row[name] = Arr.map(yield* rt.lookup(arrangement, encoded, prior), read);
            } else {
              const [first] = yield* rt.lookup(arrangement, encoded, prior, {
                limit: 1,
                reverse: a.function === "max",
              });

              if (first) row[name] = read(first);
            }
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

  return difference(before, after, fault);
});

export const top = Effect.fnUntraced(function* (node: TopNNode, program: Program, rt: Runtime) {
  const fault = rt.fault(node.id);
  const { limit } = node;
  const a = findArrangement(program, node.input, node.partitionBy ?? [], node.orderBy);
  const partitions = new Set<string>();

  for (const d of rt.changes.get(node.input)?.values() ?? [])
    for (const row of [d.before, d.after]) {
      if (!row) continue;

      const key = checkData(fault, d.key, () =>
        asRowKey(
          Arr.map(node.partitionBy ?? [], (e) =>
            evaluate(e, { row, key: d.key, parameter: rt.parameter }),
          ),
        ),
      );

      partitions.add(canonicalJson(key));
      rt.checkFanOut?.(node.id, partitions.size, key);
    }

  const before: Entry[] = [],
    after: Entry[] = [];

  for (const key of partitions) {
    before.push(...(yield* rt.lookup(a, key, true, { limit })));
    after.push(...(yield* rt.lookup(a, key, false, { limit })));
  }

  return difference(before, after, fault);
});

interface SignedOperand {
  readonly field: string;
  readonly value: number;
  readonly sign: 1 | -1;
}

/** The sum of absolute operands bounds every partial sum, even after cancellation. */
function addIntegers(old: Accumulator | undefined, operands: readonly SignedOperand[]) {
  const sums = { ...old?.sums };
  const integerMass = { ...old?.integerMass };

  for (const { field, value, sign } of operands) {
    const mass = (integerMass[field] ?? 0) + sign * Math.abs(value);

    if (!Number.isSafeInteger(value) || !Number.isSafeInteger(mass) || mass < 0) return undefined;

    integerMass[field] = mass;
    sums[field] = (sums[field] ?? 0) + sign * value;
  }

  return { sums, integerMass };
}
