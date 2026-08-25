import type {
  Expression,
  GroupedAggregateNode,
  RelationNode,
  RelationPlan,
  ReferenceScope,
} from "@streamsy/views-ir";
import { canonicalJson } from "./key.ts";

/* oxlint-disable anti-slop/no-known-value-widening, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion, unicorn/no-array-sort -- Requirements canonicalize already-checked JSON-only A1 expression trees and return named public descriptor fields; sorting mutates only fresh field arrays. */

export interface ArrangementRequirement {
  readonly id: string;
  readonly relationId: string;
  readonly keyExpression: Expression;
  readonly retainedFields: readonly string[];
  readonly ordered: boolean;
}

export interface AccumulatorRequirement {
  readonly name: string;
  readonly kind: "count" | "sum" | "max" | "count-where";
}

export interface OperatorRequirements {
  readonly nodeId: string;
  readonly inputFields: Readonly<Record<string, readonly string[]>>;
  readonly arrangements: readonly ArrangementRequirement[];
  readonly accumulators: readonly AccumulatorRequirement[];
  readonly materializeOutput: boolean;
  readonly orderedOutput: boolean;
  readonly intrinsicState: "none" | "accumulators" | "candidate-index";
}

/** Inspectable physical requirements; it never changes the public IR. */
export function planRequirements(plan: RelationPlan): readonly OperatorRequirements[] {
  const local = new Map(plan.nodes.map((node) => [node.id, requirementsFor(node)]));
  const downstream = new Map<string, Set<string>>([[plan.output, new Set(["*"])]]);
  const resolved = new Map<string, OperatorRequirements>();
  const add = (relationId: string, values: readonly string[]): void => {
    const demand = downstream.get(relationId) ?? new Set<string>();
    for (const value of values) demand.add(value);
    downstream.set(relationId, demand);
  };
  for (const node of plan.nodes.toReversed()) {
    const requirement = local.get(node.id)!;
    const output = downstream.get(node.id) ?? new Set<string>();
    const inputFields: Record<string, readonly string[]> = {};
    if (node.kind === "source") {
      inputFields[node.sourceId] = union(requirement.inputFields[node.sourceId] ?? [], output);
    } else if (node.kind === "project") {
      const selected = output.has("*")
        ? Object.values(node.fields)
        : [...output].flatMap((path) => {
            const expression = node.fields[path.split(".")[0] ?? path];
            return expression === undefined ? [] : [expression];
          });
      const values = fields(selected, "row");
      inputFields[node.input] = values;
      add(node.input, values);
    } else if (node.kind === "inner-join" || node.kind === "left-join") {
      const left = new Set(requirement.inputFields[node.left] ?? []);
      const right = new Set(requirement.inputFields[node.right] ?? []);
      if (output.has("*")) {
        left.add("*");
        right.add("*");
      } else {
        for (const path of output) {
          if (path === node.rightAlias) right.add("*");
          else if (path.startsWith(`${node.rightAlias}.`))
            right.add(path.slice(node.rightAlias.length + 1));
          else left.add(path);
        }
      }
      const leftValues = [...left].sort();
      const rightValues = [...right].sort();
      inputFields[node.left] = leftValues;
      inputFields[node.right] = rightValues;
      add(node.left, leftValues);
      add(node.right, rightValues);
    } else if (node.kind === "grouped-aggregate" || node.kind === "reduce-by-key") {
      const values = requirement.inputFields[node.input] ?? [];
      inputFields[node.input] = values;
      add(node.input, values);
    } else {
      const values = union(requirement.inputFields[node.input] ?? [], output);
      inputFields[node.input] = values;
      add(node.input, values);
    }
    resolved.set(node.id, { ...requirement, inputFields });
  }
  return plan.nodes.map((node) => resolved.get(node.id)!);
}

function union(left: Iterable<string>, right: Iterable<string>): readonly string[] {
  return [...new Set([...left, ...right])].sort();
}

function requirementsFor(node: RelationNode): OperatorRequirements {
  const inputFields: Record<string, readonly string[]> = {};
  const arrangements: ArrangementRequirement[] = [];
  let accumulators: readonly AccumulatorRequirement[] = [];
  let intrinsicState: OperatorRequirements["intrinsicState"] = "none";
  let orderedOutput = false;

  switch (node.kind) {
    case "source":
      inputFields[node.sourceId] = fields([node.key, node.partitionBy], "row");
      break;
    case "filter":
      inputFields[node.input] = fields([node.predicate], "row");
      break;
    case "project":
      inputFields[node.input] = fields(Object.values(node.fields), "row");
      break;
    case "key":
      inputFields[node.input] = fields([node.key], "row");
      break;
    case "inner-join":
    case "left-join": {
      const equality = joinEquality(node.on);
      const leftFields = fields([equality.left], "left");
      const rightFields = fields([equality.right], "right");
      inputFields[node.left] = leftFields;
      inputFields[node.right] = rightFields;
      arrangements.push(
        arrangement(node.left, equality.left, leftFields),
        arrangement(node.right, equality.right, rightFields),
      );
      break;
    }
    case "grouped-aggregate":
      inputFields[node.input] = fields(
        [
          ...Object.values(node.groupBy),
          ...Object.values(node.aggregates).flatMap((aggregate) =>
            aggregate.expression === undefined ? [] : [aggregate.expression],
          ),
        ],
        "row",
      );
      accumulators = aggregateRequirements(node);
      intrinsicState = "accumulators";
      break;
    case "top-n":
      inputFields[node.input] = fields(
        [...node.orderBy.map((term) => term.expression), ...(node.partitionBy ?? [])],
        "row",
      );
      intrinsicState = "candidate-index";
      orderedOutput = true;
      break;
    case "reduce-by-key":
      inputFields[node.input] = fields([node.key], "row");
      intrinsicState = "accumulators";
      break;
  }
  return {
    nodeId: node.id,
    inputFields,
    arrangements,
    accumulators,
    materializeOutput: node.kind !== "top-n",
    orderedOutput,
    intrinsicState,
  };
}

function arrangement(
  relationId: string,
  expression: Expression,
  retainedFields: readonly string[],
): ArrangementRequirement {
  const signature = canonicalJson(expression as never);
  return {
    id: `arrangement:${relationId}:${signature}`,
    relationId,
    keyExpression: expression,
    retainedFields,
    ordered: false,
  };
}

function aggregateRequirements(node: GroupedAggregateNode): readonly AccumulatorRequirement[] {
  return [
    { name: "$members", kind: "count" },
    ...Object.entries(node.aggregates).map(([name, aggregate]) => ({
      name,
      kind: aggregate.function,
    })),
  ];
}

function joinEquality(on: Expression): { readonly left: Expression; readonly right: Expression } {
  if (on.kind !== "binary" || on.operator !== "equal")
    throw new TypeError("A2 joins require one equality expression");
  return { left: on.left, right: on.right };
}

function fields(expressions: readonly Expression[], scope: ReferenceScope): readonly string[] {
  const paths = new Set<string>();
  for (const expression of expressions) collect(expression, scope, paths);
  return [...paths].sort();
}

function collect(expression: Expression, scope: ReferenceScope, paths: Set<string>): void {
  if (expression.kind === "reference") {
    if (expression.scope === scope && expression.path.length > 0)
      paths.add(expression.path.join("."));
    return;
  }
  if (expression.kind === "unary") collect(expression.operand, scope, paths);
  if (expression.kind === "binary") {
    collect(expression.left, scope, paths);
    collect(expression.right, scope, paths);
  }
  if (expression.kind === "variadic")
    for (const operand of expression.operands) collect(operand, scope, paths);
}

export function equalitySides(on: Expression): {
  readonly left: Expression;
  readonly right: Expression;
} {
  return joinEquality(on);
}
