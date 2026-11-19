import type { Change, JsonObject, RowKey, TopNNode } from "../../ir/contracts.ts";
import { transition } from "../change.ts";
import { compareValues, evaluate } from "../expression.ts";
import { asRowKey, encodeRowKey } from "../key.ts";
import type { TopCandidate, TopPartitionState } from "../state.ts";

/* oxlint-disable anti-slop/no-runtime-typeof, unicorn/no-array-sort -- Top limit evaluation is the runtime validation boundary; candidate arrays are newly owned and intentionally maintained in place. */

export interface OrderedPartition {
  readonly partition: RowKey;
  readonly keys: readonly RowKey[];
}

export interface TopResult {
  readonly partitions: readonly TopPartitionState[];
  readonly changes: readonly Change<JsonObject>[];
  readonly ordered: readonly OrderedPartition[];
  readonly operations: number;
}

export function topChange(
  node: TopNNode,
  current: readonly TopPartitionState[],
  change: Change<JsonObject>,
  parameters: JsonObject,
): TopResult {
  const limit = topLimit(node, parameters);
  const partitions = new Map(
    current.map((partition) => [
      encodeRowKey(partition.key),
      { key: partition.key, candidates: [...partition.candidates] },
    ]),
  );
  const affected = new Map<string, { key: RowKey; before: readonly TopCandidate[] }>();
  const remember = (row: JsonObject): void => {
    const key = partitionKey(node, row, parameters);
    const encoded = encodeRowKey(key);
    if (!affected.has(encoded))
      affected.set(encoded, {
        key,
        before: (partitions.get(encoded)?.candidates ?? []).slice(0, limit),
      });
  };
  if (change.kind !== "enter") remember(change.before);
  if (change.kind !== "exit") remember(change.after);
  if (change.kind !== "enter")
    removeCandidate(node, partitions, change.key, change.before, parameters);
  if (change.kind !== "exit") addCandidate(node, partitions, change.key, change.after, parameters);

  const changes: Change<JsonObject>[] = [];
  for (const [encoded, item] of affected) {
    const after = (partitions.get(encoded)?.candidates ?? []).slice(0, limit);
    const beforeRows = new Map(
      item.before.map((candidate) => [encodeRowKey(candidate.key), candidate]),
    );
    const afterRows = new Map(after.map((candidate) => [encodeRowKey(candidate.key), candidate]));
    const keys = new Set([...beforeRows.keys(), ...afterRows.keys()]);
    for (const key of [...keys].sort()) {
      const before = beforeRows.get(key);
      const next = afterRows.get(key);
      const output = transition(before?.key ?? next!.key, before?.row, next?.row);
      if (output !== undefined) changes.push(output);
    }
  }
  const result = [...partitions.values()]
    .filter((partition) => partition.candidates.length > 0)
    .sort((left, right) => encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)))
    .map((partition) => ({
      key: partition.key,
      candidates: partition.candidates.toSorted((left, right) =>
        compareCandidates(node, left, right),
      ),
    }));
  return {
    partitions: result,
    changes,
    ordered: result.map((partition) => ({
      partition: partition.key,
      keys: partition.candidates.slice(0, limit).map((candidate) => candidate.key),
    })),
    operations: affected.size * Math.max(1, Math.ceil(Math.log2(Math.max(2, current.length + 1)))),
  };
}

export function orderedTop(
  node: TopNNode,
  current: readonly TopPartitionState[],
  parameters: JsonObject,
): readonly OrderedPartition[] {
  const limit = topLimit(node, parameters);
  return current.map((partition) => ({
    partition: partition.key,
    keys: partition.candidates.slice(0, limit).map((candidate) => candidate.key),
  }));
}

function addCandidate(
  node: TopNNode,
  partitions: Map<string, { key: RowKey; candidates: TopCandidate[] }>,
  key: RowKey,
  row: JsonObject,
  parameters: JsonObject,
): void {
  const partition = partitionKey(node, row, parameters);
  const encoded = encodeRowKey(partition);
  const state = partitions.get(encoded) ?? { key: partition, candidates: [] };
  if (state.candidates.some((candidate) => encodeRowKey(candidate.key) === encodeRowKey(key)))
    throw new TypeError("top candidate enter duplicates an existing row key");
  const sortValues = node.orderBy.map((term) =>
    evaluate(term.expression, { row, parameter: parameters }),
  );
  for (const value of sortValues) compareValues(value, value);
  state.candidates.push({
    key,
    row,
    sortValues,
  });
  state.candidates.sort((left, right) => compareCandidates(node, left, right));
  partitions.set(encoded, state);
}

function removeCandidate(
  node: TopNNode,
  partitions: Map<string, { key: RowKey; candidates: TopCandidate[] }>,
  key: RowKey,
  row: JsonObject,
  parameters: JsonObject,
): void {
  const partition = partitionKey(node, row, parameters);
  const encoded = encodeRowKey(partition);
  const state = partitions.get(encoded);
  if (state === undefined) throw new TypeError("top retraction references an absent partition");
  const index = state.candidates.findIndex(
    (candidate) => encodeRowKey(candidate.key) === encodeRowKey(key),
  );
  if (index < 0) throw new TypeError("top retraction references an absent candidate");
  state.candidates.splice(index, 1);
  if (state.candidates.length === 0) partitions.delete(encoded);
}

function compareCandidates(node: TopNNode, left: TopCandidate, right: TopCandidate): number {
  for (const [index, term] of node.orderBy.entries()) {
    const comparison = compareValues(left.sortValues[index]!, right.sortValues[index]!);
    if (comparison !== 0) return term.direction === "ascending" ? comparison : -comparison;
  }
  return encodeRowKey(left.key).localeCompare(encodeRowKey(right.key));
}

function partitionKey(node: TopNNode, row: JsonObject, parameters: JsonObject): RowKey {
  return asRowKey(
    (node.partitionBy ?? []).map((expression) =>
      evaluate(expression, { row, parameter: parameters }),
    ),
  );
}

function topLimit(node: TopNNode, parameters: JsonObject): number {
  const value = evaluate(node.limit, { parameter: parameters });
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0 || value > node.maximum)
    throw new TypeError(`top limit must be an integer between 1 and ${node.maximum}`);
  return value;
}
