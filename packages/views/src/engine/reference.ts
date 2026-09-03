import type {
  DescriptorRef,
  Expression,
  GroupedAggregateNode,
  JsonObject,
  JsonValue,
  RelationPlan,
  RowKey,
  TopNNode,
} from "../ir/contracts.ts";
import { compareValues, evaluate, evaluateOptional, isMissing } from "./expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "./key.ts";
import type { OrderedPartition } from "./operators/top.ts";
import type { StateRow } from "./state.ts";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-known-value-widening, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion, unicorn/no-array-sort -- The independent oracle validates dynamic expression results directly; open JSON rows and canonicalization assertions mirror the accepted JSON-only A1 boundary, while sorts only mutate fresh arrays. */

export interface FullRecomputeInput {
  readonly plan: RelationPlan;
  readonly sources: Readonly<Record<string, readonly StateRow[]>>;
  readonly parameters?: JsonObject;
  readonly decodeRow?: (schema: DescriptorRef, row: JsonObject, nodeId: string) => JsonObject;
}

export interface FullRecomputeResult {
  readonly rows: readonly StateRow[];
  readonly ordered: readonly OrderedPartition[];
}

/** Direct collection evaluator. It deliberately imports no transition operator. */
export function fullRecompute(input: FullRecomputeInput): FullRecomputeResult {
  const relations = new Map<string, readonly StateRow[]>();
  const ordered = new Map<string, readonly OrderedPartition[]>();
  const parameters = input.parameters ?? {};
  for (const node of input.plan.nodes) {
    switch (node.kind) {
      case "source":
        relations.set(
          node.id,
          sourceRows(
            input.sources[node.sourceId] ?? input.sources[node.id] ?? [],
            node.key,
            parameters,
          ),
        );
        break;
      case "filter":
        relations.set(
          node.id,
          rows(relations, node.input).filter((entry) => {
            const result = evaluate(node.predicate, { row: entry.row, parameter: parameters });
            if (typeof result !== "boolean") throw new TypeError("filter predicate is not Boolean");
            return result;
          }),
        );
        break;
      case "project":
        relations.set(
          node.id,
          rows(relations, node.input).map((entry) => {
            const row: Record<string, JsonValue> = {};
            for (const [name, expression] of Object.entries(node.fields))
              row[name] = evaluate(expression, { row: entry.row, parameter: parameters });
            return {
              key: entry.key,
              row: input.decodeRow?.(node.schema, row, node.id) ?? row,
            };
          }),
        );
        break;
      case "key":
        relations.set(
          node.id,
          rows(relations, node.input).map((entry) => ({
            key: asRowKey(evaluate(node.key, { row: entry.row, parameter: parameters })),
            row: entry.row,
          })),
        );
        break;
      case "inner-join":
      case "left-join": {
        const output: StateRow[] = [];
        for (const left of rows(relations, node.left)) {
          let matched = false;
          for (const right of rows(relations, node.right)) {
            const accepted = evaluate(node.on, {
              left: left.row,
              right: right.row,
              parameter: parameters,
            });
            if (typeof accepted !== "boolean") throw new TypeError("join condition is not Boolean");
            if (!accepted) continue;
            matched = true;
            output.push({
              key: ["match", encodeRowKey(left.key), encodeRowKey(right.key)],
              row: { ...left.row, [node.rightAlias]: right.row },
            });
          }
          if (!matched && node.kind === "left-join")
            output.push({ key: ["unmatched", encodeRowKey(left.key)], row: left.row });
        }
        relations.set(node.id, sortRows(output));
        break;
      }
      case "grouped-aggregate":
        relations.set(node.id, recomputeAggregate(node, rows(relations, node.input), parameters));
        break;
      case "top-n": {
        const result = recomputeTop(node, rows(relations, node.input), parameters);
        relations.set(node.id, result.rows);
        ordered.set(node.id, result.ordered);
        break;
      }
      case "reduce-by-key":
        throw new TypeError(
          "full recomputation expects normalized relation changes after fact reduction",
        );
    }
  }
  return {
    rows: sortRows(rows(relations, input.plan.output)),
    ordered: ordered.get(input.plan.output) ?? [],
  };
}

function sourceRows(
  source: readonly StateRow[],
  keyExpression: Expression,
  parameters: JsonObject,
): readonly StateRow[] {
  for (const entry of source) {
    const key = asRowKey(evaluate(keyExpression, { row: entry.row, parameter: parameters }));
    if (encodeRowKey(key) !== encodeRowKey(entry.key))
      throw new TypeError("source row key disagrees with the source key expression");
  }
  return source;
}

function recomputeAggregate(
  node: GroupedAggregateNode,
  input: readonly StateRow[],
  parameters: JsonObject,
): readonly StateRow[] {
  const groups = new Map<string, { key: RowKey; group: JsonObject; members: JsonObject[] }>();
  for (const entry of input) {
    const group: Record<string, JsonValue> = {};
    const values: JsonValue[] = [];
    for (const [name, expression] of Object.entries(node.groupBy)) {
      const value = evaluate(expression, { row: entry.row, parameter: parameters });
      group[name] = value;
      values.push(value);
    }
    const key = asRowKey(values);
    const identity = encodeRowKey(key);
    const bucket = groups.get(identity) ?? { key, group, members: [] };
    bucket.members.push(entry.row);
    groups.set(identity, bucket);
  }
  return sortRows(
    [...groups.values()].map((bucket) => {
      const row: Record<string, JsonValue> = { ...bucket.group };
      for (const [name, aggregate] of Object.entries(node.aggregates)) {
        if (aggregate.function === "count") {
          row[name] = bucket.members.length;
          continue;
        }
        if (aggregate.expression === undefined) throw new TypeError("aggregate operand is absent");
        const values = bucket.members.map((member) =>
          evaluateOptional(aggregate.expression!, { row: member, parameter: parameters }),
        );
        if (aggregate.function === "count-where") {
          row[name] = values.filter((value) => {
            if (isMissing(value) || typeof value !== "boolean")
              throw new TypeError("count-where requires Booleans");
            return value;
          }).length;
        } else if (aggregate.function === "sum") {
          row[name] = values.reduce<number>((sum, value) => {
            if (isMissing(value)) return sum;
            if (typeof value !== "number" || !Number.isFinite(value))
              throw new TypeError("sum requires finite numbers");
            return sum + value;
          }, 0);
        } else {
          const present = values.filter((value): value is JsonValue => !isMissing(value));
          if (present.length > 0) row[name] = present.toSorted(compareValues).at(-1)!;
        }
      }
      return { key: bucket.key, row };
    }),
  );
}

function recomputeTop(
  node: TopNNode,
  input: readonly StateRow[],
  parameters: JsonObject,
): FullRecomputeResult {
  const limit = evaluate(node.limit, { parameter: parameters });
  if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0 || limit > node.maximum)
    throw new TypeError("invalid top limit");
  const partitions = new Map<
    string,
    { key: RowKey; rows: { entry: StateRow; sort: JsonValue[] }[] }
  >();
  for (const entry of input) {
    const key = asRowKey(
      (node.partitionBy ?? []).map((expression) =>
        evaluate(expression, { row: entry.row, parameter: parameters }),
      ),
    );
    const identity = encodeRowKey(key);
    const partition = partitions.get(identity) ?? { key, rows: [] };
    partition.rows.push({
      entry,
      sort: node.orderBy.map((term) =>
        evaluate(term.expression, { row: entry.row, parameter: parameters }),
      ),
    });
    partitions.set(identity, partition);
  }
  const winners: StateRow[] = [];
  const ordered: OrderedPartition[] = [];
  for (const partition of [...partitions.values()].sort((left, right) =>
    encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)),
  )) {
    const selected = partition.rows
      .toSorted((left, right) => {
        for (const [index, term] of node.orderBy.entries()) {
          const comparison = compareValues(left.sort[index]!, right.sort[index]!);
          if (comparison !== 0) return term.direction === "ascending" ? comparison : -comparison;
        }
        return encodeRowKey(left.entry.key).localeCompare(encodeRowKey(right.entry.key));
      })
      .slice(0, limit);
    winners.push(...selected.map((candidate) => candidate.entry));
    ordered.push({
      partition: partition.key,
      keys: selected.map((candidate) => candidate.entry.key),
    });
  }
  return { rows: winners, ordered };
}

function rows(relations: Map<string, readonly StateRow[]>, id: string): readonly StateRow[] {
  const value = relations.get(id);
  if (value === undefined) throw new TypeError(`relation ${id} has not been evaluated`);
  return value;
}

function sortRows(input: readonly StateRow[]): readonly StateRow[] {
  return [...input].sort((left, right) =>
    encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)),
  );
}

export function normalizeResult(result: FullRecomputeResult): string {
  return canonicalJson({ rows: sortRows(result.rows), ordered: result.ordered } as never);
}
