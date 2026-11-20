import type {
  DescriptorRef,
  Expression,
  SqlGroupedAggregateNode,
  JsonObject,
  JsonValue,
  SqlRelationPlan,
  SqlRelationNode,
  RowKey,
  TopNNode,
} from "../ir/contracts.ts";
import { evaluate, evaluateOptional, isMissing } from "../runtime/expression.ts";
import type { Evaluated, ExpressionScopes } from "../runtime/expression.ts";
import { asRowKey, canonicalJson, encodeRowKey } from "../runtime/key.ts";

import { dataFault } from "./data-fault.ts";
import type { Program } from "./planner.ts";
import type { Accumulator, Entry as StateRow } from "./store.ts";
import { Record as Rec, Array as Arr } from "effect";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-known-value-widening, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion, unicorn/no-array-sort -- The independent oracle validates dynamic expression results directly; open JSON rows and canonicalization assertions mirror the accepted JSON-only A1 boundary, while sorts only mutate fresh arrays. */

export interface OrderedPartition {
  readonly partition: RowKey;
  readonly keys: readonly RowKey[];
}

export interface FullRecomputeInput {
  readonly plan: SqlRelationPlan;
  readonly sources: Readonly<Record<string, readonly StateRow[]>>;
  readonly parameters?: ExpressionScopes["parameter"];
  readonly decodeRow?: (schema: DescriptorRef, row: JsonObject, nodeId: string) => JsonObject;
}

export interface FullRecomputeResult {
  readonly rows: readonly StateRow[];
  readonly ordered: readonly OrderedPartition[];
}

/** Direct collection evaluator. It deliberately imports no transition operator. */
export function fullRecompute(
  input: FullRecomputeInput,
  stateOutputKey = false,
): FullRecomputeResult {
  const { relations, ordered } = evaluateRelations(input.plan.nodes, input, {
    location: (node) => ({ view: input.plan.name, node }),
    stateKeys: new Set(stateOutputKey ? [input.plan.output] : []),
  });

  return {
    rows: sortRows(rows(relations, input.plan.output)),
    ordered: ordered.get(input.plan.output) ?? [],
  };
}

/** Read the R1 before image from the stored inputs and the actual R1 sum results. */
export function legacyRelations(
  program: Program,
  sources: FullRecomputeInput["sources"],
  accumulators: ReadonlyMap<string, ReadonlyMap<string, Accumulator>>,
  parameters: ExpressionScopes["parameter"],
) {
  return evaluateRelations(
    program.nodes,
    { sources, parameters },
    {
      location: (node) => program.locations.get(node) ?? { view: "*", node },
      stateKeys: program.stateKeys,
      accumulators,
    },
  ).relations;
}

interface EvaluationOptions {
  readonly location: (node: string) => { readonly view: string; readonly node: string };
  readonly stateKeys?: ReadonlySet<string>;
  readonly accumulators?: ReadonlyMap<string, ReadonlyMap<string, Accumulator>>;
}

function evaluateRelations(
  nodes: readonly SqlRelationNode[],
  input: Omit<FullRecomputeInput, "plan">,
  options: EvaluationOptions,
) {
  const relations = new Map<string, readonly StateRow[]>();
  const ordered = new Map<string, readonly OrderedPartition[]>();
  const parameters = input.parameters ?? {};

  for (const node of nodes) {
    switch (node.kind) {
      case "source":
        relations.set(
          node.id,
          sourceRows(
            input.sources[node.sourceId] ?? input.sources[node.id] ?? [],
            node.key,
            parameters,
            node.stateKey,
          ),
        );
        break;
      case "filter":
        relations.set(
          node.id,
          Arr.filter(rows(relations, node.input), (entry) => {
            const result = evaluate(node.predicate, {
              row: entry.row,
              key: entry.key,
              parameter: parameters,
            });

            if (typeof result !== "boolean") throw new TypeError("filter predicate is not Boolean");

            return result;
          }),
        );
        break;
      case "project":
        relations.set(
          node.id,
          Arr.map(rows(relations, node.input), (entry) => {
            const row: Record<string, JsonValue> = {};

            for (const [name, expression] of Rec.toEntries(node.fields)) {
              const value = evaluateOptional(expression, {
                row: entry.row,
                key: entry.key,
                parameter: parameters,
              });

              if (!isMissing(value)) row[name] = value;
            }

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
          Arr.map(rows(relations, node.input), (entry) => {
            const key = asRowKey(
              evaluate(node.key, { row: entry.row, key: entry.key, parameter: parameters }),
            );

            return { key: options.stateKeys?.has(node.id) ? String(key) : key, row: entry.row };
          }),
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
        relations.set(
          node.id,
          recomputeAggregate(
            node,
            rows(relations, node.input),
            parameters,
            options.accumulators?.get(node.id),
          ),
        );
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

    const seen = new Set<string>();

    for (const row of rows(relations, node.id)) {
      const key = encodeRowKey(row.key);

      if (seen.has(key))
        throw dataFault(options.location(node.id))(row.key, "duplicate output key");
      seen.add(key);
    }
  }

  return { relations, ordered };
}

function sourceRows(
  source: readonly StateRow[],
  keyExpression: Expression,
  parameters: NonNullable<ExpressionScopes["parameter"]>,
  stateKey?: true,
): readonly StateRow[] {
  for (const entry of source) {
    const value = evaluate(keyExpression, {
      row: entry.row,
      key: entry.key,
      parameter: parameters,
    });

    const key = stateKey ? String(asRowKey(value)) : asRowKey(value);

    if (encodeRowKey(key) !== encodeRowKey(entry.key))
      throw new TypeError("source row key disagrees with the source key expression");
  }

  return source;
}

function recomputeAggregate(
  node: SqlGroupedAggregateNode,
  input: readonly StateRow[],
  parameters: NonNullable<ExpressionScopes["parameter"]>,
  stored?: ReadonlyMap<string, Accumulator>,
): readonly StateRow[] {
  const groups = new Map<string, { key: RowKey; group: JsonObject; members: StateRow[] }>();

  for (const entry of input) {
    const group: Record<string, JsonValue> = {};
    const values: JsonValue[] = [];

    for (const [name, expression] of Rec.toEntries(node.groupBy)) {
      const value = evaluate(expression, { row: entry.row, key: entry.key, parameter: parameters });
      group[name] = value;
      values.push(value);
    }

    const key = asRowKey(values);
    const identity = encodeRowKey(key);
    const bucket = groups.get(identity) ?? { key, group, members: [] };
    bucket.members.push(entry);
    groups.set(identity, bucket);
  }

  return sortRows(
    Arr.map([...groups.values()], (bucket) => {
      const row: Record<string, JsonValue> = { ...bucket.group };

      for (const [name, aggregate] of Rec.toEntries(node.aggregates)) {
        if (aggregate.function === "count") {
          row[name] = bucket.members.length;
          continue;
        }

        if (aggregate.function === "sum" && stored !== undefined) {
          row[name] = stored.get(canonicalJson(bucket.key))?.sums[name] ?? 0;
          continue;
        }

        if (aggregate.expression === undefined) throw new TypeError("aggregate operand is absent");

        const members =
          aggregate.function === "sum"
            ? [...bucket.members].sort((a, b) => {
                const left = encodeRowKey(a.key),
                  right = encodeRowKey(b.key);

                return left < right ? -1 : left > right ? 1 : 0;
              })
            : bucket.members;

        const values = Arr.map(members, (member) =>
          evaluateOptional(aggregate.expression!, {
            row: member.row,
            key: member.key,
            parameter: parameters,
          }),
        );

        if (aggregate.function === "count-where") {
          row[name] = Arr.filter(values, (value) => {
            if (isMissing(value) || typeof value !== "boolean")
              throw new TypeError("count-where requires Booleans");

            return value;
          }).length;
        } else if (aggregate.function === "sum") {
          row[name] = Arr.reduce(values, 0, (sum, value) => {
            if (isMissing(value)) return sum;

            if (typeof value !== "number" || !Number.isFinite(value))
              throw new TypeError("sum requires finite numbers");

            return sum + value;
          });

          if (!Number.isFinite(row[name])) throw new TypeError("sum overflow");
        } else {
          const present = Arr.filter(values, (value): value is JsonValue => !isMissing(value));

          if (present.length > 0)
            row[name] = present.sort(compareValues).at(aggregate.function === "min" ? 0 : -1)!;
        }
      }

      return { key: bucket.key, row };
    }),
  );
}

function recomputeTop(
  node: TopNNode,
  input: readonly StateRow[],
  parameters: NonNullable<ExpressionScopes["parameter"]>,
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
      Arr.map(node.partitionBy ?? [], (expression) =>
        evaluate(expression, { row: entry.row, key: entry.key, parameter: parameters }),
      ),
    );

    const identity = encodeRowKey(key);
    const partition = partitions.get(identity) ?? { key, rows: [] };
    partition.rows.push({
      entry,
      sort: Arr.map(node.orderBy, (term) =>
        evaluate(term.expression, { row: entry.row, key: entry.key, parameter: parameters }),
      ),
    });
    partitions.set(identity, partition);
  }

  const winners: StateRow[] = [];
  const ordered: OrderedPartition[] = [];

  for (const partition of [...partitions.values()].sort((left, right) =>
    encodeRowKey(left.key).localeCompare(encodeRowKey(right.key)),
  )) {
    const selected = [...partition.rows]
      .sort((left, right) => {
        for (const [index, term] of node.orderBy.entries()) {
          const comparison = compareValues(left.sort[index]!, right.sort[index]!);

          if (comparison !== 0) return term.direction === "ascending" ? comparison : -comparison;
        }

        const a = encodeRowKey(left.entry.key),
          b = encodeRowKey(right.entry.key);

        return a < b ? -1 : a > b ? 1 : 0;
      })
      .slice(0, limit);

    winners.push(...Arr.map(selected, (candidate) => candidate.entry));
    ordered.push({
      partition: partition.key,
      keys: Arr.map(selected, (candidate) => candidate.entry.key),
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

/** Independent scalar comparator: deliberately does not import the SQL key codec. */
function compareValues(left: Evaluated, right: Evaluated): number {
  if (isMissing(left)) return isMissing(right) ? 0 : 1;

  if (isMissing(right)) return -1;

  if (
    typeof left === "number" &&
    typeof right === "number" &&
    Number.isFinite(left) &&
    Number.isFinite(right)
  )
    return left - right;

  if (typeof left === "string" && typeof right === "string")
    return left < right ? -1 : left > right ? 1 : 0;

  if (typeof left === "boolean" && typeof right === "boolean") return Number(left) - Number(right);
  throw new TypeError("order requires finite comparable scalars of one type");
}
