import { Effect, Schema } from "effect";
import type {
  Expression,
  ReferenceScope,
  RelationNode,
  RelationPlan,
  TopNNode,
} from "@streamsy/views-ir";
import type { ViewDeclaration } from "./relation.ts";
import { encodePlan, planHash } from "./plan.ts";

export const PlanIssueCode = Schema.Literals([
  "duplicate-name",
  "duplicate-node-id",
  "unknown-input",
  "schema-mismatch",
  "invalid-expression-scope",
  "non-boolean-predicate",
  "invalid-output-key",
  "join-alias-conflict",
  "join-condition-not-boolean",
  "aggregate-shape-mismatch",
  "undeclared-parameter",
  "unbounded-top",
  "invalid-top-limit",
  "unstable-top-order",
  "unsupported-plan-version",
  "non-json-plan-value",
]);
export type PlanIssueCode = typeof PlanIssueCode.Type;

export const PlanIssue = Schema.Struct({
  code: PlanIssueCode,
  view: Schema.String,
  nodeId: Schema.optionalKey(Schema.String),
  path: Schema.String,
  message: Schema.String,
});
export type PlanIssue = typeof PlanIssue.Type;

export class PlanCheckFailed extends Schema.TaggedError<PlanCheckFailed>()("PlanCheckFailed", {
  issues: Schema.Array(PlanIssue),
}) {}

export interface CheckedPlan {
  readonly plan: RelationPlan;
  readonly hash: string;
}

type CheckInput = ViewDeclaration | RelationPlan;

export function collectPlanIssues(input: CheckInput): readonly PlanIssue[] {
  const plan = "plan" in input ? input.plan : input;
  const view = plan.name;
  const issues: PlanIssue[] = [];
  const add = (code: PlanIssueCode, path: string, message: string, nodeId?: string): void => {
    const issue = { code, view, path, message };
    issues.push(nodeId === undefined ? issue : { ...issue, nodeId });
  };

  try {
    encodePlan(plan);
  } catch (cause) {
    add("non-json-plan-value", "$plan", cause instanceof Error ? cause.message : String(cause));
  }

  if (plan.version !== 1)
    add("unsupported-plan-version", "version", `unsupported plan version ${String(plan.version)}`);
  const nodes = Array.isArray(plan.nodes) ? plan.nodes : [];
  const ids = new Set<string>();
  const sourceNames = new Set<string>();
  for (const [nodeIndex, node] of nodes.entries()) {
    const path = `nodes[${nodeIndex}]`;
    if (ids.has(node.id))
      add("duplicate-node-id", `${path}.id`, `duplicate node id ${node.id}`, node.id);
    ids.add(node.id);
    if (node.kind === "source") {
      if (sourceNames.has(node.sourceId))
        add(
          "duplicate-name",
          `${path}.sourceId`,
          `duplicate source name ${node.sourceId}`,
          node.id,
        );
      sourceNames.add(node.sourceId);
    }
    if (
      node.schema.name.length === 0 ||
      !Number.isInteger(node.schema.version) ||
      node.schema.version < 1
    ) {
      add(
        "schema-mismatch",
        `${path}.schema`,
        "schema descriptors need a name and positive integer version",
        node.id,
      );
    }
  }
  if (!ids.has(plan.output))
    add("invalid-output-key", "output", `output ${plan.output} is not a node`);

  const aliases = new Set<string>();
  for (const [nodeIndex, node] of nodes.entries()) {
    const path = `nodes[${nodeIndex}]`;
    for (const [inputPath, inputId] of inputsOf(node)) {
      if (!ids.has(inputId))
        add("unknown-input", `${path}.${inputPath}`, `unknown input ${inputId}`, node.id);
    }
    for (const [expressionPath, expression] of expressionsOf(node)) {
      checkExpression(
        expression,
        allowedScopes(node),
        `${path}.${expressionPath}`,
        plan,
        add,
        node.id,
      );
    }
    if (node.kind === "filter" && !couldBeBoolean(node.predicate)) {
      add(
        "non-boolean-predicate",
        `${path}.predicate`,
        "filter predicates must be boolean",
        node.id,
      );
    }
    if (node.kind === "inner-join" || node.kind === "left-join") {
      if (node.rightAlias.length === 0 || aliases.has(node.rightAlias)) {
        add(
          "join-alias-conflict",
          `${path}.rightAlias`,
          "join aliases must be non-empty and unique",
          node.id,
        );
      }
      aliases.add(node.rightAlias);
      if (!couldBeBoolean(node.on))
        add("join-condition-not-boolean", `${path}.on`, "join conditions must be boolean", node.id);
    }
    if (node.kind === "grouped-aggregate") {
      if (Object.keys(node.groupBy).length === 0 || Object.keys(node.aggregates).length === 0) {
        add(
          "aggregate-shape-mismatch",
          path,
          "grouped aggregates need group and aggregate fields",
          node.id,
        );
      }
      for (const name of Object.keys(node.aggregates)) {
        const aggregate = node.aggregates[name];
        if (aggregate === undefined) continue;
        const needsExpression = aggregate.function !== "count";
        if (needsExpression !== (aggregate.expression !== undefined)) {
          add(
            "aggregate-shape-mismatch",
            `${path}.aggregates.${name}`,
            `${aggregate.function} has an invalid operand shape`,
            node.id,
          );
        }
      }
    }
    if (node.kind === "top-n") checkTop(node, path, plan, add);
  }
  return Object.freeze(
    issues.toSorted((left, right) =>
      `${left.nodeId ?? ""}|${left.path}|${left.code}`.localeCompare(
        `${right.nodeId ?? ""}|${right.path}|${right.code}`,
      ),
    ),
  );
}

export const checkPlan = (input: CheckInput): Effect.Effect<CheckedPlan, PlanCheckFailed> => {
  const plan = "plan" in input ? input.plan : input;
  const issues = collectPlanIssues(input);
  return issues.length === 0
    ? Effect.succeed(Object.freeze({ plan, hash: planHash(plan) }))
    : Effect.fail(new PlanCheckFailed({ issues }));
};

function inputsOf(node: RelationNode): readonly (readonly [string, string])[] {
  switch (node.kind) {
    case "source":
      return [];
    case "inner-join":
    case "left-join":
      return [
        ["left", node.left],
        ["right", node.right],
      ];
    default:
      return [["input", node.input]];
  }
}

function expressionsOf(node: RelationNode): readonly (readonly [string, Expression])[] {
  switch (node.kind) {
    case "source":
      return [
        ["key", node.key],
        ["order", node.order],
        ["partitionBy", node.partitionBy],
      ];
    case "filter":
      return [["predicate", node.predicate]];
    case "project":
      return Object.entries(node.fields).map(([key, value]) => [`fields.${key}`, value]);
    case "key":
      return [["key", node.key]];
    case "inner-join":
    case "left-join":
      return [["on", node.on]];
    case "grouped-aggregate":
      return [
        ...Object.entries(node.groupBy).map(([key, value]) => [`groupBy.${key}`, value] as const),
        ...Object.entries(node.aggregates).flatMap(([key, value]) =>
          value.expression === undefined
            ? []
            : [[`aggregates.${key}.expression`, value.expression] as const],
        ),
      ];
    case "top-n":
      return [
        ["limit", node.limit],
        ...node.orderBy.map((term, index) => [`orderBy[${index}]`, term.expression] as const),
        ...(node.partitionBy ?? []).map(
          (expression, index) => [`partitionBy[${index}]`, expression] as const,
        ),
      ];
    case "reduce-by-key":
      return [
        ["key", node.key],
        ["order", node.order],
      ];
  }
}

function allowedScopes(node: RelationNode): ReadonlySet<ReferenceScope> {
  switch (node.kind) {
    case "inner-join":
    case "left-join":
      return new Set(["left", "right", "parameter"]);
    case "reduce-by-key":
      return new Set(["row", "event", "state", "key", "parameter"]);
    default:
      return new Set(["row", "key", "parameter"]);
  }
}

function checkExpression(
  expression: Expression,
  scopes: ReadonlySet<ReferenceScope>,
  path: string,
  plan: RelationPlan,
  add: (code: PlanIssueCode, path: string, message: string, nodeId?: string) => void,
  nodeId: string,
): void {
  if (expression.kind === "reference") {
    if (!scopes.has(expression.scope))
      add("invalid-expression-scope", path, `scope ${expression.scope} is invalid here`, nodeId);
    if (expression.scope === "parameter") {
      const name = expression.path[0];
      if (name === undefined || plan.parameters?.[name] === undefined)
        add(
          "undeclared-parameter",
          path,
          `parameter ${name ?? "<missing>"} is not declared`,
          nodeId,
        );
    }
    return;
  }
  if (expression.kind === "unary")
    return checkExpression(expression.operand, scopes, `${path}.operand`, plan, add, nodeId);
  if (expression.kind === "binary") {
    checkExpression(expression.left, scopes, `${path}.left`, plan, add, nodeId);
    checkExpression(expression.right, scopes, `${path}.right`, plan, add, nodeId);
    return;
  }
  if (expression.kind === "variadic") {
    expression.operands.forEach((operand, index) =>
      checkExpression(operand, scopes, `${path}.operands[${index}]`, plan, add, nodeId),
    );
  }
}

function couldBeBoolean(expression: Expression): boolean {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- LiteralExpression.value is parsed JsonValue; this distinguishes its boolean arm.
  if (expression.kind === "literal") return typeof expression.value === "boolean";
  if (expression.kind === "reference") return true;
  if (expression.kind === "unary")
    return expression.operator === "is-present" || expression.operator === "not";
  if (expression.kind === "binary") return !["add", "or-else"].includes(expression.operator);
  return expression.operator !== "key";
}

function checkTop(
  node: TopNNode,
  path: string,
  plan: RelationPlan,
  add: (code: PlanIssueCode, path: string, message: string, nodeId?: string) => void,
): void {
  if (!Number.isInteger(node.maximum) || node.maximum <= 0)
    add("unbounded-top", `${path}.maximum`, "top needs a finite positive maximum", node.id);
  if (node.limit.kind === "literal") {
    if (
      // oxlint-disable-next-line anti-slop/no-runtime-typeof -- LiteralExpression.value is parsed JsonValue; top accepts only its number arm.
      typeof node.limit.value !== "number" ||
      !Number.isInteger(node.limit.value) ||
      node.limit.value <= 0 ||
      node.limit.value > node.maximum
    ) {
      add(
        "invalid-top-limit",
        `${path}.limit`,
        "top limit must be a positive integer within its maximum",
        node.id,
      );
    }
  } else if (node.limit.kind === "reference" && node.limit.scope === "parameter") {
    const parameter = plan.parameters?.[node.limit.path[0] ?? ""];
    if (parameter?.maximum === undefined || parameter.maximum !== node.maximum)
      add("unbounded-top", `${path}.limit`, "top parameter needs a matching maximum", node.id);
  } else {
    add("invalid-top-limit", `${path}.limit`, "top limit must be a literal or parameter", node.id);
  }
  const last = node.orderBy.at(-1);
  const lastName = last?.expression.kind === "reference" ? last.expression.path.at(-1) : undefined;
  if (
    last === undefined ||
    last.direction !== "ascending" ||
    last.expression.kind !== "reference" ||
    (last.expression.scope !== "key" && !lastName?.toLowerCase().endsWith("id"))
  ) {
    add(
      "unstable-top-order",
      `${path}.orderBy`,
      "top needs a final ascending stable key term",
      node.id,
    );
  }
}
