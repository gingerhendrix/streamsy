import { isJsonObject, type Expression, type JsonObject, type JsonValue } from "../ir/contracts.ts";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion, typescript/consistent-return -- This module is the pure runtime parser for JSON expression results; its guards establish each operator's scalar contract, and assertions only recover array/object evidence erased by indexed JSON access. */

const Missing = Symbol("missing");
export type Evaluated = JsonValue | typeof Missing;

export function isMissing(value: Evaluated): value is typeof Missing {
  return value === Missing;
}

export interface ExpressionScopes {
  readonly row?: JsonValue;
  readonly left?: JsonValue;
  readonly right?: JsonValue;
  readonly key?: JsonValue;
  readonly event?: JsonValue;
  readonly state?: JsonValue;
  readonly parameter?: JsonValue;
}

export class ExpressionFault extends TypeError {
  constructor(
    readonly expression: Expression,
    detail: string,
  ) {
    super(detail);
    this.name = "ExpressionFault";
  }
}

export function evaluate(expression: Expression, scopes: ExpressionScopes): JsonValue {
  const value = evaluateOptional(expression, scopes);
  if (value === Missing) throw new ExpressionFault(expression, "expression result is absent");
  return value;
}

export function evaluateOptional(expression: Expression, scopes: ExpressionScopes): Evaluated {
  switch (expression.kind) {
    case "literal":
      return expression.value;
    case "reference":
      return reference(expression, scopes);
    case "unary": {
      const operand = evaluateOptional(expression.operand, scopes);
      if (expression.operator === "is-present") return operand !== Missing;
      if (expression.operator === "value") return operand;
      if (operand === Missing || typeof operand !== "boolean")
        throw new ExpressionFault(expression, "not requires a present Boolean");
      return !operand;
    }
    case "binary":
      return binary(expression, scopes);
    case "variadic":
      return variadic(expression, scopes);
  }
}

function reference(
  expression: Extract<Expression, { readonly kind: "reference" }>,
  scopes: ExpressionScopes,
): Evaluated {
  const root = scopes[expression.scope];
  if (root === undefined) return Missing;
  let current: JsonValue = root;
  for (const segment of expression.path) {
    if (!isJsonObject(current)) return Missing;
    const child: JsonValue | undefined = current[segment];
    if (child === undefined) return Missing;
    current = child;
  }
  return current;
}

function binary(
  expression: Extract<Expression, { readonly kind: "binary" }>,
  scopes: ExpressionScopes,
): Evaluated {
  const left = evaluateOptional(expression.left, scopes);
  if (expression.operator === "or-else")
    return left === Missing ? evaluateOptional(expression.right, scopes) : left;
  const right = evaluateOptional(expression.right, scopes);
  if (left === Missing || right === Missing) {
    if (expression.operator === "equal") return false;
    if (expression.operator === "not-equal") return true;
    return Missing;
  }
  switch (expression.operator) {
    case "equal":
      return equal(left, right);
    case "not-equal":
      return !equal(left, right);
    case "greater-than":
      return compareScalar(left, right, expression) > 0;
    case "greater-than-or-equal":
      return compareScalar(left, right, expression) >= 0;
    case "less-than":
      return compareScalar(left, right, expression) < 0;
    case "less-than-or-equal":
      return compareScalar(left, right, expression) <= 0;
    case "add":
      if (typeof left !== "number" || typeof right !== "number")
        throw new ExpressionFault(expression, "add requires finite numbers");
      if (!Number.isFinite(left + right))
        throw new ExpressionFault(expression, "add produced a non-finite number");
      return left + right;
  }
}

function variadic(
  expression: Extract<Expression, { readonly kind: "variadic" }>,
  scopes: ExpressionScopes,
): Evaluated {
  const operands = expression.operands.map((operand) => evaluateOptional(operand, scopes));
  if (expression.operator === "key") {
    if (operands.some((value) => value === Missing)) return Missing;
    return operands as JsonValue[];
  }
  if (expression.operator === "in") {
    const [subject, ...candidates] = operands;
    return (
      subject !== undefined &&
      subject !== Missing &&
      candidates.some((candidate) => candidate !== Missing && equal(subject, candidate))
    );
  }
  if (operands.some((value) => value === Missing || typeof value !== "boolean"))
    throw new ExpressionFault(expression, `${expression.operator} requires present Booleans`);
  return expression.operator === "and" ? operands.every(Boolean) : operands.some(Boolean);
}

function equal(left: JsonValue, right: JsonValue): boolean {
  if (left === right) return true;
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object")
    return false;
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => equal(value, right[index] as JsonValue))
    );
  }
  const leftEntries = Object.entries(left);
  const rightEntries = Object.entries(right);
  return (
    leftEntries.length === rightEntries.length &&
    isJsonObject(right) &&
    leftEntries.every(([key, value]) => key in right && equal(value, right[key] as JsonValue))
  );
}

export function compareValues(left: Evaluated, right: Evaluated): number {
  if (left === Missing) return right === Missing ? 0 : 1;
  if (right === Missing) return -1;
  return compareScalar(left, right);
}

function compareScalar(left: JsonValue, right: JsonValue, expression?: Expression): number {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right))
      throw new ExpressionFault(expression ?? literalNull, "comparison requires finite numbers");
    return left - right;
  }
  if (typeof left === "string" && typeof right === "string") return left.localeCompare(right);
  if (typeof left === "boolean" && typeof right === "boolean") return Number(left) - Number(right);
  throw new ExpressionFault(
    expression ?? literalNull,
    "values are not comparable scalars of one type",
  );
}

const literalNull: Expression = { kind: "literal", value: null };

export function requireObject(value: JsonValue, context: string): JsonObject {
  if (!isJsonObject(value)) throw new TypeError(`${context} requires a JSON object row`);
  return value;
}
