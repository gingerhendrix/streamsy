import type {
  Change,
  DescriptorRef,
  Expression,
  JsonObject,
  JsonValue,
  RelationNode,
  RelationPlan,
  RowKey,
} from "@streamsy/views-ir";
import { coalesceChanges, sameRow } from "./change.ts";
import { OperatorFault, type OperatorPhase } from "./errors.ts";
import { evaluate, evaluateOptional, isMissing } from "./expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "./key.ts";
import { aggregateChange, aggregateRows } from "./operators/grouped-aggregate.ts";
import { filterChange } from "./operators/filter.ts";
import { joinChange, type KeyedRow } from "./operators/join.ts";
import { projectChange } from "./operators/project.ts";
import { orderedTop, topChange, type OrderedPartition } from "./operators/top.ts";
import { planRequirements, type ArrangementRequirement } from "./requirements.ts";
import {
  emptyOperatorState,
  type AggregateGroupState,
  type AggregateState,
  type ArrangementState,
  type OperatorIndexMutation,
  type OperatorMutationPatch,
  type OperatorStateSnapshot,
  type OperatorValueMutation,
  type StateRow,
  type TopPartitionState,
  type TopState,
} from "./state.ts";

/* oxlint-disable anti-slop/no-known-value-widening, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion, unicorn/no-array-sort, typescript/consistent-return -- The graph owns all sorted arrays it mutates; assertions bridge the checked JSON-only A1 IR to canonical serialization and exhaustive node dispatch. */

export interface SourceChanges {
  readonly sourceId: string;
  readonly changes: readonly Change<JsonObject>[];
}

export interface MaintainGraphInput {
  readonly plan: RelationPlan;
  readonly state?: OperatorStateSnapshot;
  readonly inputs: readonly SourceChanges[];
  readonly parameters?: JsonObject;
  readonly decodeRow?: (schema: DescriptorRef, row: JsonObject, nodeId: string) => JsonObject;
}

export interface OperationCounts {
  readonly changes: number;
  readonly indexLookups: number;
  readonly candidateComparisons: number;
  readonly aggregateContributions: number;
}

export interface MaintainGraphResult {
  readonly state: OperatorStateSnapshot;
  readonly patch: OperatorMutationPatch;
  readonly changes: readonly Change<JsonObject>[];
  readonly rows: readonly StateRow[];
  readonly ordered: readonly OrderedPartition[];
  readonly operations: OperationCounts;
}

interface Runtime {
  readonly relations: Map<string, Map<string, KeyedRow>>;
  readonly arrangements: Map<string, MutableArrangement>;
  readonly aggregates: Map<string, readonly AggregateGroupState[]>;
  readonly tops: Map<string, readonly TopPartitionState[]>;
}

interface MutableArrangement {
  readonly requirement: ArrangementRequirement;
  readonly buckets: Map<string, { value: JsonValue; rows: Map<string, RowKey> }>;
}

/** Apply normalized complete-row changes through an acyclic A1 graph. */
export function maintainGraph(input: MaintainGraphInput): MaintainGraphResult {
  const prior = input.state ?? emptyOperatorState(input.plan.name);
  const parameters = input.parameters ?? {};
  let runtime: Runtime;
  try {
    validateSnapshot(input.plan, prior);
    runtime = restore(input.plan, prior);
  } catch (cause) {
    if (cause instanceof OperatorFault) throw cause;
    throw fault(
      input.plan,
      undefined,
      "restore",
      cause instanceof Error ? cause.message : String(cause),
      undefined,
      cause,
    );
  }
  const consumers = consumersOf(input.plan);
  const emitted: Change<JsonObject>[] = [];
  const counts = {
    changes: 0,
    indexLookups: 0,
    candidateComparisons: 0,
    aggregateContributions: 0,
  };

  for (const sourceInput of input.inputs) {
    const source = input.plan.nodes.find(
      (node): node is Extract<RelationNode, { readonly kind: "source" }> =>
        node.kind === "source" &&
        (node.sourceId === sourceInput.sourceId || node.id === sourceInput.sourceId),
    );
    if (source === undefined)
      throw fault(input.plan, undefined, "plan", `unknown source ${sourceInput.sourceId}`);
    for (const sourceChange of sourceInput.changes) {
      validateSourceKey(input.plan, source, sourceChange, parameters);
      applyRelationChange(input.plan, source, runtime, sourceChange);
      const queue: { relationId: string; change: Change<JsonObject> }[] = [
        { relationId: source.id, change: sourceChange },
      ];
      while (queue.length > 0) {
        const item = queue.shift()!;
        for (const node of consumers.get(item.relationId) ?? []) {
          let changes: readonly Change<JsonObject>[];
          try {
            const result = processNode(
              input,
              runtime,
              node,
              item.relationId,
              item.change,
              parameters,
            );
            changes = result.changes;
            counts.indexLookups += result.indexLookups;
            counts.candidateComparisons += result.candidateComparisons;
            counts.aggregateContributions += result.aggregateContributions;
          } catch (cause) {
            if (cause instanceof OperatorFault) throw cause;
            throw fault(
              input.plan,
              node,
              phaseOf(node),
              cause instanceof Error ? cause.message : String(cause),
              item.change.key,
              cause,
            );
          }
          counts.changes += changes.length;
          if (node.id === input.plan.output) emitted.push(...changes);
          if (node.kind === "top-n") continue;
          for (const change of changes) {
            applyRelationChange(input.plan, node, runtime, change);
            queue.push({ relationId: node.id, change });
          }
        }
      }
    }
  }

  const state = snapshot(input.plan, prior.revision + 1, runtime);
  const output = input.plan.nodes.find((node) => node.id === input.plan.output);
  if (output === undefined) throw fault(input.plan, undefined, "plan", "output node is absent");
  const rows = outputRows(output, runtime, parameters);
  const ordered =
    output.kind === "top-n"
      ? orderedTop(output, runtime.tops.get(output.id) ?? [], parameters)
      : [];
  return {
    state,
    patch: mutationPatch(prior, state),
    changes: coalesceChanges(emitted),
    rows,
    ordered,
    operations: counts,
  };
}

function validateSourceKey(
  plan: RelationPlan,
  source: Extract<RelationNode, { readonly kind: "source" }>,
  change: Change<JsonObject>,
  parameters: JsonObject,
): void {
  const rows =
    change.kind === "enter"
      ? [change.after]
      : change.kind === "exit"
        ? [change.before]
        : [change.before, change.after];
  for (const row of rows) {
    const key = asRowKey(evaluate(source.key, { row, parameter: parameters }));
    if (encodeRowKey(key) !== encodeRowKey(change.key))
      throw fault(
        plan,
        source,
        "key",
        "change key disagrees with the source key expression",
        change.key,
      );
  }
}

function processNode(
  input: MaintainGraphInput,
  runtime: Runtime,
  node: RelationNode,
  changedInput: string,
  change: Change<JsonObject>,
  parameters: JsonObject,
): {
  readonly changes: readonly Change<JsonObject>[];
  readonly indexLookups: number;
  readonly candidateComparisons: number;
  readonly aggregateContributions: number;
} {
  const decode = (row: JsonObject): JsonObject =>
    input.decodeRow?.(node.schema, row, node.id) ?? row;
  switch (node.kind) {
    case "filter":
      return metrics(filterChange(node, change, parameters));
    case "project":
      return metrics(projectChange(node, change, parameters, decode));
    case "key":
      return metrics(keyChange(node.key, change, parameters));
    case "inner-join":
    case "left-join": {
      const result = joinChange(
        node,
        changedInput,
        change,
        {
          relation: (relationId) => [...(runtime.relations.get(relationId)?.values() ?? [])],
          lookup: (relationId, expression, value) => lookup(runtime, relationId, expression, value),
        },
        parameters,
      );
      return { ...metrics(result.changes), indexLookups: result.operations };
    }
    case "grouped-aggregate": {
      const result = aggregateChange(
        node,
        runtime.aggregates.get(node.id) ?? [],
        change,
        parameters,
      );
      runtime.aggregates.set(node.id, result.groups);
      return { ...metrics(result.changes), aggregateContributions: result.operations };
    }
    case "top-n": {
      const result = topChange(node, runtime.tops.get(node.id) ?? [], change, parameters);
      runtime.tops.set(node.id, result.partitions);
      return { ...metrics(result.changes), candidateComparisons: result.operations };
    }
    case "source":
      throw new TypeError("source nodes cannot consume graph changes");
    case "reduce-by-key":
      throw new TypeError("reduce-by-key consumes fact items through the Slice 1 reducer host");
  }
}

function metrics(changes: readonly Change<JsonObject>[]) {
  return { changes, indexLookups: 0, candidateComparisons: 0, aggregateContributions: 0 };
}

function keyChange(
  expression: Expression,
  change: Change<JsonObject>,
  parameters: JsonObject,
): readonly Change<JsonObject>[] {
  if (change.kind === "enter") {
    const key = asRowKey(evaluate(expression, { row: change.after, parameter: parameters }));
    return [{ kind: "enter", key, after: change.after }];
  }
  const beforeKey = asRowKey(evaluate(expression, { row: change.before, parameter: parameters }));
  if (change.kind === "exit") return [{ kind: "exit", key: beforeKey, before: change.before }];
  const afterKey = asRowKey(evaluate(expression, { row: change.after, parameter: parameters }));
  if (encodeRowKey(beforeKey) !== encodeRowKey(afterKey))
    return [
      { kind: "exit", key: beforeKey, before: change.before },
      { kind: "enter", key: afterKey, after: change.after },
    ];
  return sameRow(change.before, change.after)
    ? []
    : [{ kind: "update", key: afterKey, before: change.before, after: change.after }];
}

function applyRelationChange(
  plan: RelationPlan,
  node: RelationNode,
  runtime: Runtime,
  change: Change<JsonObject>,
): void {
  const relation = runtime.relations.get(node.id) ?? new Map<string, KeyedRow>();
  const encoded = encodeRowKey(change.key);
  const existing = relation.get(encoded);
  if (change.kind === "enter") {
    if (existing !== undefined)
      throw fault(plan, node, "restore", "enter duplicates an existing key", change.key);
    relation.set(encoded, { key: change.key, row: change.after });
  } else {
    if (existing === undefined || !sameRow(existing.row, change.before))
      throw fault(
        plan,
        node,
        "restore",
        "change before-row does not match current relation",
        change.key,
      );
    if (change.kind === "exit") relation.delete(encoded);
    else relation.set(encoded, { key: change.key, row: change.after });
  }
  runtime.relations.set(node.id, relation);
  updateArrangements(runtime, node.id, change);
}

function restore(plan: RelationPlan, state: OperatorStateSnapshot): Runtime {
  const relations = new Map(
    state.relations.map((relation) => [
      relation.relationId,
      new Map(relation.rows.map((entry) => [encodeRowKey(entry.key), entry])),
    ]),
  );
  const requirements = planRequirements(plan).flatMap((item) => item.arrangements);
  const saved = new Map(state.arrangements.map((arrangement) => [arrangement.id, arrangement]));
  const arrangements = new Map<string, MutableArrangement>();
  for (const requirement of uniqueArrangements(requirements)) {
    const prior = saved.get(requirement.id);
    const mutable: MutableArrangement = { requirement, buckets: new Map() };
    if (prior !== undefined) {
      for (const entry of prior.entries)
        mutable.buckets.set(canonicalJson(entry.value), {
          value: entry.value,
          rows: new Map(entry.rowKeys.map((key) => [encodeRowKey(key), key])),
        });
    } else {
      for (const entry of relations.get(requirement.relationId)?.values() ?? [])
        arrangementAdd(mutable, entry.key, entry.row);
    }
    arrangements.set(requirement.id, mutable);
  }
  for (const node of plan.nodes) {
    if (node.kind !== "grouped-aggregate" || relations.has(node.id)) continue;
    const groups = state.aggregates.find((aggregate) => aggregate.nodeId === node.id)?.groups ?? [];
    relations.set(
      node.id,
      new Map(aggregateRows(node, groups).map((entry) => [encodeRowKey(entry.key), entry])),
    );
  }
  return {
    relations,
    arrangements,
    aggregates: new Map(state.aggregates.map((aggregate) => [aggregate.nodeId, aggregate.groups])),
    tops: new Map(state.tops.map((top) => [top.nodeId, top.partitions])),
  };
}

function updateArrangements(
  runtime: Runtime,
  relationId: string,
  change: Change<JsonObject>,
): void {
  for (const arrangement of runtime.arrangements.values()) {
    if (arrangement.requirement.relationId !== relationId) continue;
    if (change.kind !== "enter") arrangementRemove(arrangement, change.key, change.before);
    if (change.kind !== "exit") arrangementAdd(arrangement, change.key, change.after);
  }
}

function arrangementAdd(arrangement: MutableArrangement, key: RowKey, row: JsonObject): void {
  const value = evaluateOptional(arrangement.requirement.keyExpression, {
    row,
    left: row,
    right: row,
  });
  if (isMissing(value)) return;
  const encoded = canonicalJson(value);
  const bucket = arrangement.buckets.get(encoded) ?? { value, rows: new Map() };
  bucket.rows.set(encodeRowKey(key), key);
  arrangement.buckets.set(encoded, bucket);
}

function arrangementRemove(arrangement: MutableArrangement, key: RowKey, row: JsonObject): void {
  const value = evaluateOptional(arrangement.requirement.keyExpression, {
    row,
    left: row,
    right: row,
  });
  if (isMissing(value)) return;
  const encoded = canonicalJson(value);
  const bucket = arrangement.buckets.get(encoded);
  if (bucket === undefined || !bucket.rows.delete(encodeRowKey(key)))
    throw new TypeError("arrangement retraction references an absent entry");
  if (bucket.rows.size === 0) arrangement.buckets.delete(encoded);
}

function lookup(
  runtime: Runtime,
  relationId: string,
  expression: Expression,
  value: JsonValue,
): readonly KeyedRow[] {
  const signature = canonicalJson(expression as never);
  const arrangement = [...runtime.arrangements.values()].find(
    (candidate) =>
      candidate.requirement.relationId === relationId &&
      canonicalJson(candidate.requirement.keyExpression as never) === signature,
  );
  if (arrangement === undefined) throw new TypeError(`missing arrangement for ${relationId}`);
  const keys = arrangement.buckets.get(canonicalJson(value))?.rows.values() ?? [];
  const relation = runtime.relations.get(relationId);
  return [...keys]
    .map((key) => relation?.get(encodeRowKey(key)))
    .filter((row): row is KeyedRow => row !== undefined);
}

function snapshot(plan: RelationPlan, revision: number, runtime: Runtime): OperatorStateSnapshot {
  const derivedIds = new Set(
    plan.nodes
      .filter((node) => node.kind === "top-n" || node.kind === "grouped-aggregate")
      .map((node) => node.id),
  );
  return {
    version: 1,
    planName: plan.name,
    revision,
    relations: [...runtime.relations]
      .filter(([relationId]) => !derivedIds.has(relationId))
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([relationId, rows]) => ({
        relationId,
        rows: [...rows.values()].sort((left, right) =>
          encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)),
        ),
      })),
    arrangements: [...runtime.arrangements.values()]
      .sort((left, right) => left.requirement.id.localeCompare(right.requirement.id))
      .map(arrangementSnapshot),
    aggregates: [...runtime.aggregates]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([nodeId, groups]): AggregateState => ({ nodeId, groups })),
    tops: [...runtime.tops]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([nodeId, partitions]): TopState => ({ nodeId, partitions })),
  };
}

function arrangementSnapshot(arrangement: MutableArrangement): ArrangementState {
  return {
    id: arrangement.requirement.id,
    relationId: arrangement.requirement.relationId,
    retainedFields: arrangement.requirement.retainedFields,
    entries: [...arrangement.buckets.values()]
      .sort((left, right) => canonicalJson(left.value).localeCompare(canonicalJson(right.value)))
      .map((entry) => ({
        value: entry.value,
        rowKeys: [...entry.rows.values()].sort((left, right) =>
          encodeRowKey(left).localeCompare(encodeRowKey(right)),
        ),
      })),
  };
}

function outputRows(
  node: RelationNode,
  runtime: Runtime,
  parameters: JsonObject,
): readonly StateRow[] {
  if (node.kind !== "top-n")
    return [...(runtime.relations.get(node.id)?.values() ?? [])].sort((left, right) =>
      encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)),
    );
  const ordered = orderedTop(node, runtime.tops.get(node.id) ?? [], parameters);
  const candidates = new Map(
    (runtime.tops.get(node.id) ?? []).flatMap((partition) =>
      partition.candidates.map((candidate) => [encodeRowKey(candidate.key), candidate] as const),
    ),
  );
  return ordered
    .flatMap((partition) =>
      partition.keys.map((key) => ({ key, row: candidates.get(encodeRowKey(key))!.row })),
    )
    .sort((left, right) => encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)));
}

function consumersOf(plan: RelationPlan): Map<string, RelationNode[]> {
  const consumers = new Map<string, RelationNode[]>();
  const add = (input: string, node: RelationNode): void => {
    const values = consumers.get(input) ?? [];
    values.push(node);
    consumers.set(input, values);
  };
  for (const node of plan.nodes) {
    if (node.kind === "source") continue;
    if (node.kind === "inner-join" || node.kind === "left-join") {
      add(node.left, node);
      if (node.right !== node.left) add(node.right, node);
    } else add(node.input, node);
  }
  return consumers;
}

function uniqueArrangements(
  requirements: readonly ArrangementRequirement[],
): readonly ArrangementRequirement[] {
  return [...new Map(requirements.map((requirement) => [requirement.id, requirement])).values()];
}

function validateSnapshot(plan: RelationPlan, state: OperatorStateSnapshot): void {
  if (state.version !== 1 || state.planName !== plan.name)
    throw fault(
      plan,
      undefined,
      "restore",
      "operator-state snapshot is incompatible with the plan",
    );
  const ids = new Set(plan.nodes.map((node) => node.id));
  const relationRows = new Map(
    state.relations.map((relation) => [
      relation.relationId,
      new Set(relation.rows.map((entry) => encodeRowKey(entry.key))),
    ]),
  );
  for (const relation of state.relations)
    if (!ids.has(relation.relationId))
      throw fault(
        plan,
        undefined,
        "restore",
        `state references unknown node ${relation.relationId}`,
      );
  for (const aggregate of state.aggregates) {
    if (!ids.has(aggregate.nodeId))
      throw fault(plan, undefined, "restore", `state references unknown node ${aggregate.nodeId}`);
    if (
      aggregate.groups.some(
        (group) =>
          group.memberCount <= 0 ||
          Object.values(group.maxima).some((values) => values.some((entry) => entry.count <= 0)),
      )
    )
      throw fault(plan, undefined, "restore", `aggregate ${aggregate.nodeId} has corrupt counts`);
  }
  for (const top of state.tops)
    if (!ids.has(top.nodeId))
      throw fault(plan, undefined, "restore", `state references unknown node ${top.nodeId}`);
  const arrangementIds = new Set(
    planRequirements(plan)
      .flatMap((requirement) => requirement.arrangements)
      .map((arrangement) => arrangement.id),
  );
  for (const arrangement of state.arrangements) {
    if (!arrangementIds.has(arrangement.id))
      throw fault(
        plan,
        undefined,
        "restore",
        `state references unknown arrangement ${arrangement.id}`,
      );
    const rowKeys = relationRows.get(arrangement.relationId);
    if (
      arrangement.entries.length > 0 &&
      (rowKeys === undefined ||
        arrangement.entries.some((entry) =>
          entry.rowKeys.some((rowKey) => !rowKeys.has(encodeRowKey(rowKey))),
        ))
    )
      throw fault(
        plan,
        undefined,
        "restore",
        `arrangement ${arrangement.id} references an absent row`,
      );
  }
}

function mutationPatch(
  before: OperatorStateSnapshot,
  after: OperatorStateSnapshot,
): OperatorMutationPatch {
  const oldValues = valueRecords(before);
  const newValues = valueRecords(after);
  const oldIndexes = indexRecords(before);
  const newIndexes = indexRecords(after);
  const operatorValues: OperatorValueMutation[] = [];
  const operatorIndexes: OperatorIndexMutation[] = [];
  for (const [identity, old] of oldValues)
    if (!newValues.has(identity))
      operatorValues.push({ id: old.id, operation: "delete", key: old.key });
  for (const [identity, next] of newValues)
    if (canonicalJson(oldValues.get(identity)?.value ?? null) !== canonicalJson(next.value))
      operatorValues.push({ id: next.id, operation: "put", key: next.key, value: next.value });
  for (const [identity, old] of oldIndexes)
    if (!newIndexes.has(identity))
      operatorIndexes.push({
        id: old.id,
        operation: "delete",
        indexName: old.indexName,
        indexKey: old.indexKey,
        sortKey: old.sortKey,
        rowKey: old.rowKey,
      });
  for (const [identity, next] of newIndexes) {
    const old = oldIndexes.get(identity);
    if (old === undefined || canonicalJson(old.value ?? null) !== canonicalJson(next.value ?? null))
      operatorIndexes.push({ ...next, operation: "put" });
  }
  return {
    version: 1,
    planName: after.planName,
    baseRevision: before.revision,
    nextRevision: after.revision,
    operatorValues,
    operatorIndexes,
  };
}

function valueRecords(state: OperatorStateSnapshot) {
  const records = new Map<string, { id: string; key: RowKey; value: JsonValue }>();
  for (const aggregate of state.aggregates)
    for (const group of aggregate.groups)
      records.set(`${aggregate.nodeId}|${encodeRowKey(group.key)}`, {
        id: aggregate.nodeId,
        key: group.key,
        value: group as never,
      });
  return records;
}

function indexRecords(state: OperatorStateSnapshot) {
  type Put = Extract<OperatorIndexMutation, { operation: "put" }>;
  const records = new Map<string, Omit<Put, "operation">>();
  for (const arrangement of state.arrangements)
    for (const entry of arrangement.entries)
      for (const rowKey of entry.rowKeys) {
        const indexKey = asRowKey(entry.value);
        const value = { id: arrangement.id, indexName: "exact", indexKey, sortKey: rowKey, rowKey };
        records.set(`${arrangement.id}|${encodeRowKey(indexKey)}|${encodeRowKey(rowKey)}`, value);
      }
  for (const top of state.tops)
    for (const partition of top.partitions)
      for (const candidate of partition.candidates) {
        const sortKey = asRowKey(candidate.sortValues);
        const value = {
          id: top.nodeId,
          indexName: "candidates",
          indexKey: partition.key,
          sortKey,
          rowKey: candidate.key,
          value: candidate.row as JsonValue,
        };
        records.set(
          `${top.nodeId}|${encodeRowKey(partition.key)}|${encodeRowKey(sortKey)}|${encodeRowKey(candidate.key)}`,
          value,
        );
      }
  return records;
}

function phaseOf(node: RelationNode): OperatorPhase {
  switch (node.kind) {
    case "filter":
      return "predicate";
    case "project":
      return "project";
    case "key":
      return "key";
    case "inner-join":
    case "left-join":
      return "join";
    case "grouped-aggregate":
      return "aggregate";
    case "top-n":
      return "top";
    case "source":
    case "reduce-by-key":
      return "plan";
  }
}

function fault(
  plan: RelationPlan,
  node: RelationNode | undefined,
  phase: OperatorPhase,
  detail: string,
  rowKey?: RowKey,
  cause?: unknown,
): OperatorFault {
  return new OperatorFault({
    planName: plan.name,
    nodeId: node?.id ?? "$graph",
    operatorKind: node?.kind ?? "graph",
    phase,
    detail,
    rowKey,
    cause,
  });
}
