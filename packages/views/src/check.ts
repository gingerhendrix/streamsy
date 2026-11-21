import { Schema, Record as Rec, Array as Arr, Predicate } from "effect";
import type {
  Expression,
  ReferenceScope,
  RelationNode,
  RelationPlan,
  TopNNode,
} from "./ir/contracts.ts";

export const PlanIssueCode = Schema.Literals([
  "invalid-expression-scope",
  "non-boolean-predicate",
  "join-alias-conflict",
  "join-condition-not-boolean",
  "aggregate-shape-mismatch",
  "undeclared-parameter",
  "invalid-top-limit",
  "invalid-closure-depth",
  "invalid-closure-key",
  "unstable-top-order",
  "unsupported-sql-plan",
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

export class PlanCheckFailed extends Schema.TaggedError<
  PlanCheckFailed,
  { readonly brand: unique symbol }
>()("PlanCheckFailed", {
  issues: Schema.Array(PlanIssue),
}) {}

export function collectPlanIssues(plan: RelationPlan): readonly PlanIssue[] {
  const view = plan.name;
  const issues: PlanIssue[] = [];

  const add = (code: PlanIssueCode, path: string, message: string, nodeId?: string): void => {
    const issue = { code, view, path, message };
    issues.push(nodeId === undefined ? issue : { ...issue, nodeId });
  };

  const aliases = new Set<string>();
  const nonStringKeys = new Set<string>();

  for (const [nodeIndex, node] of plan.nodes.entries()) {
    const path = `nodes[${nodeIndex}]`;

    if (hasNonStringKey(node, nonStringKeys)) nonStringKeys.add(node.id);

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
      if (Rec.keys(node.groupBy).length === 0 || Rec.keys(node.aggregates).length === 0) {
        add(
          "aggregate-shape-mismatch",
          path,
          "grouped aggregates need group and aggregate fields",
          node.id,
        );
      }

      for (const name of Rec.keys(node.aggregates)) {
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

    if (node.kind === "top-n") checkTop(node, path, add);

    if (node.kind === "closure") {
      if (!Number.isInteger(node.maxDepth) || node.maxDepth <= 0)
        add(
          "invalid-closure-depth",
          `${path}.maxDepth`,
          "closure maxDepth must be a positive integer",
          node.id,
        );

      if (nonStringKeys.has(node.input))
        add("invalid-closure-key", `${path}.input`, "closure requires string row keys", node.id);

      if (!couldBeString(node.parent))
        add(
          "invalid-closure-key",
          `${path}.parent`,
          "closure requires string parent keys",
          node.id,
        );
    }
  }

  issues.sort((left, right) =>
    `${left.nodeId ?? ""}|${left.path}|${left.code}`.localeCompare(
      `${right.nodeId ?? ""}|${right.path}|${right.code}`,
    ),
  );

  return Object.freeze(issues);
}

function expressionsOf(node: RelationNode): readonly (readonly [string, Expression])[] {
  switch (node.kind) {
    case "source":
    case "key":
      return [["key", node.key]];
    case "filter":
      return [["predicate", node.predicate]];
    case "project":
      return node.row === undefined
        ? Arr.map(Rec.toEntries(node.fields), ([key, value]) => [`fields.${key}`, value])
        : [["row", node.row]];
    case "inner-join":
    case "left-join":
      return [["on", node.on]];
    case "concat":
      return [];
    case "closure":
      return node.part === undefined
        ? [["parent", node.parent]]
        : [
            ["parent", node.parent],
            ["part", node.part],
          ];
    case "grouped-aggregate":
      return [
        ...Arr.map(
          Rec.toEntries(node.groupBy),
          ([key, value]) => [`groupBy.${key}`, value] as const,
        ),
        ...Arr.flatMap(Rec.toEntries(node.aggregates), ([key, value]) =>
          value.expression === undefined
            ? []
            : [[`aggregates.${key}.expression`, value.expression] as const],
        ),
      ];
    default:
      return [
        ...Arr.map(node.orderBy, (term, index) => [`orderBy[${index}]`, term.expression] as const),
        ...Arr.map(
          node.partitionBy ?? [],
          (expression, index) => [`partitionBy[${index}]`, expression] as const,
        ),
      ];
  }
}

function hasNonStringKey(node: RelationNode, known: ReadonlySet<string>): boolean {
  switch (node.kind) {
    case "source":
      return node.stateKey !== true && !couldBeString(node.key);
    case "key":
      return !couldBeString(node.key);
    case "filter":
    case "project":
    case "top-n":
      return known.has(node.input);
    default:
      return true;
  }
}

function couldBeString(expression: Expression): boolean {
  switch (expression.kind) {
    case "literal":
      return Predicate.isString(expression.value);
    case "reference":
    case "call":
    case "field":
      return true;
    case "unary":
      return expression.operator === "unwrap" && couldBeString(expression.operand);
    case "binary":
      return (
        expression.operator === "or-else" &&
        (couldBeString(expression.left) || couldBeString(expression.right))
      );
    default:
      return false;
  }
}

function allowedScopes(node: RelationNode): ReadonlySet<ReferenceScope> {
  switch (node.kind) {
    case "inner-join":
    case "left-join":
      return new Set(["left", "right", "parameter"]);
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

      if (name === undefined || !plan.parameters.includes(name))
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

  if (expression.kind === "field")
    return checkExpression(expression.object, scopes, `${path}.object`, plan, add, nodeId);

  if (expression.kind === "binary") {
    checkExpression(expression.left, scopes, `${path}.left`, plan, add, nodeId);
    checkExpression(expression.right, scopes, `${path}.right`, plan, add, nodeId);

    return;
  }

  if (expression.kind === "variadic" || expression.kind === "call") {
    Arr.forEach(expression.operands, (operand, index) =>
      checkExpression(operand, scopes, `${path}.operands[${index}]`, plan, add, nodeId),
    );
  }
}

function couldBeBoolean(expression: Expression): boolean {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- LiteralExpression.value is parsed JsonValue; this distinguishes its boolean arm.
  if (expression.kind === "literal") return typeof expression.value === "boolean";

  if (expression.kind === "reference" || expression.kind === "field" || expression.kind === "call")
    return true;

  if (expression.kind === "unary")
    return expression.operator !== "unwrap" || couldBeBoolean(expression.operand);

  if (expression.kind === "binary") return !["add", "or-else"].includes(expression.operator);

  return expression.operator !== "key";
}

function checkTop(
  node: TopNNode,
  path: string,
  add: (code: PlanIssueCode, path: string, message: string, nodeId?: string) => void,
): void {
  if (!Number.isInteger(node.limit) || node.limit <= 0)
    add("invalid-top-limit", `${path}.limit`, "top limit must be a positive integer", node.id);

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
