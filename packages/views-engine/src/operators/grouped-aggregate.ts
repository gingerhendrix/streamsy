import type {
  Change,
  GroupedAggregateNode,
  JsonObject,
  JsonValue,
  RowKey,
} from "@streamsy/views-ir";
import { transition } from "../change.ts";
import { compareValues, evaluate, evaluateOptional, isMissing } from "../expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "../key.ts";
import type { AggregateGroupState } from "../state.ts";
import type { StateRow } from "../state.ts";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-known-value-widening, unicorn/no-array-sort -- Aggregate operands are validated at this pure runtime boundary; open JSON rows are the A1 contract, and the one mutable sort orders a newly owned result array. */

export interface AggregateResult {
  readonly groups: readonly AggregateGroupState[];
  readonly changes: readonly Change<JsonObject>[];
  readonly operations: number;
}

export function aggregateChange(
  node: GroupedAggregateNode,
  current: readonly AggregateGroupState[],
  change: Change<JsonObject>,
  parameters: JsonObject,
): AggregateResult {
  const groups = new Map(current.map((group) => [encodeRowKey(group.key), mutable(group)]));
  const affected = new Map<string, { key: RowKey; before?: JsonObject }>();
  const remember = (row: JsonObject): void => {
    const identity = groupIdentity(node, row, parameters);
    if (!affected.has(identity.encoded)) {
      const group = groups.get(identity.encoded);
      affected.set(identity.encoded, {
        key: identity.key,
        before: group === undefined ? undefined : derive(node, group),
      });
    }
  };
  if (change.kind !== "enter") remember(change.before);
  if (change.kind !== "exit") remember(change.after);
  if (change.kind !== "enter") contribute(node, groups, change.before, parameters, -1);
  if (change.kind !== "exit") contribute(node, groups, change.after, parameters, 1);

  const changes: Change<JsonObject>[] = [];
  for (const [encoded, item] of affected) {
    const next = groups.get(encoded);
    const output = transition(
      item.key,
      item.before,
      next === undefined ? undefined : derive(node, next),
    );
    if (output !== undefined) changes.push(output);
  }
  return {
    groups: [...groups.values()]
      .sort((left, right) => encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)))
      .map(frozen),
    changes,
    operations: (change.kind === "update" ? 2 : 1) * Object.keys(node.aggregates).length,
  };
}

/** Rebuild the derived aggregate relation from accumulator-only state. */
export function aggregateRows(
  node: GroupedAggregateNode,
  groups: readonly AggregateGroupState[],
): readonly StateRow[] {
  return groups.map((group) => ({ key: group.key, row: derive(node, mutable(group)) }));
}

interface MutableGroup {
  key: RowKey;
  group: JsonObject;
  memberCount: number;
  sums: Record<string, number>;
  maxima: Record<string, Map<string, { value: JsonValue; count: number }>>;
  conditionalCounts: Record<string, number>;
}

function contribute(
  node: GroupedAggregateNode,
  groups: Map<string, MutableGroup>,
  row: JsonObject,
  parameters: JsonObject,
  difference: 1 | -1,
): void {
  const identity = groupIdentity(node, row, parameters);
  let group = groups.get(identity.encoded);
  if (group === undefined) {
    if (difference < 0) throw new TypeError("aggregate retraction references an absent group");
    group = {
      key: identity.key,
      group: identity.group,
      memberCount: 0,
      sums: {},
      maxima: {},
      conditionalCounts: {},
    };
    groups.set(identity.encoded, group);
  }
  group.memberCount += difference;
  for (const [name, aggregate] of Object.entries(node.aggregates)) {
    if (aggregate.function === "count") continue;
    const expression = aggregate.expression;
    if (expression === undefined) throw new TypeError(`${aggregate.function} requires an operand`);
    const value = evaluateOptional(expression, { row, parameter: parameters });
    if (aggregate.function === "count-where") {
      if (isMissing(value) || typeof value !== "boolean")
        throw new TypeError("count-where requires a present Boolean");
      if (value) group.conditionalCounts[name] = (group.conditionalCounts[name] ?? 0) + difference;
      continue;
    }
    if (isMissing(value)) continue;
    if (aggregate.function === "sum") {
      if (typeof value !== "number" || !Number.isFinite(value))
        throw new TypeError("sum requires finite numeric values");
      const sum = (group.sums[name] ?? 0) + difference * value;
      if (!Number.isFinite(sum)) throw new TypeError("sum produced a non-finite value");
      group.sums[name] = Object.is(sum, -0) ? 0 : sum;
      continue;
    }
    if (typeof value !== "number" && typeof value !== "string")
      throw new TypeError("max requires present ordered scalar values");
    const values = (group.maxima[name] ??= new Map());
    const encoded = canonicalJson(value);
    const entry = values.get(encoded);
    const count = (entry?.count ?? 0) + difference;
    if (count < 0) throw new TypeError("max multiset count became negative");
    if (count === 0) values.delete(encoded);
    else values.set(encoded, { value, count });
  }
  if (group.memberCount < 0) throw new TypeError("aggregate member count became negative");
  if (group.memberCount === 0) groups.delete(identity.encoded);
}

function groupIdentity(
  node: GroupedAggregateNode,
  row: JsonObject,
  parameters: JsonObject,
): { readonly encoded: string; readonly key: RowKey; readonly group: JsonObject } {
  const group: Record<string, JsonValue> = {};
  const values: JsonValue[] = [];
  for (const [name, expression] of Object.entries(node.groupBy)) {
    const value = evaluate(expression, { row, parameter: parameters });
    group[name] = value;
    values.push(value);
  }
  const key = asRowKey(values);
  return { encoded: encodeRowKey(key), key, group };
}

function derive(node: GroupedAggregateNode, group: MutableGroup): JsonObject {
  const row: Record<string, JsonValue> = { ...group.group };
  for (const [name, aggregate] of Object.entries(node.aggregates)) {
    switch (aggregate.function) {
      case "count":
        row[name] = group.memberCount;
        break;
      case "count-where":
        row[name] = group.conditionalCounts[name] ?? 0;
        break;
      case "sum":
        row[name] = group.sums[name] ?? 0;
        break;
      case "max": {
        const values = [...(group.maxima[name]?.values() ?? [])];
        if (values.length > 0)
          row[name] = values
            .toSorted((left, right) => compareValues(left.value, right.value))
            .at(-1)!.value;
        break;
      }
    }
  }
  return row;
}

function mutable(group: AggregateGroupState): MutableGroup {
  return {
    key: group.key,
    group: group.group,
    memberCount: group.memberCount,
    sums: { ...group.sums },
    conditionalCounts: { ...group.conditionalCounts },
    maxima: Object.fromEntries(
      Object.entries(group.maxima).map(([name, values]) => [
        name,
        new Map(values.map((entry) => [canonicalJson(entry.value), { ...entry }])),
      ]),
    ),
  };
}

function frozen(group: MutableGroup): AggregateGroupState {
  return {
    key: group.key,
    group: group.group,
    memberCount: group.memberCount,
    sums: group.sums,
    conditionalCounts: group.conditionalCounts,
    maxima: Object.fromEntries(
      Object.entries(group.maxima).map(([name, values]) => [
        name,
        [...values.values()].toSorted((left, right) => compareValues(left.value, right.value)),
      ]),
    ),
  };
}
