import type { ESTree } from "@oxlint/plugins";

/** The discriminant field that Effect data types and tagged errors share. */
export const TAG_FIELD = "_tag";

const QUOTES = new Set(["'", '"']);

/** Removes type-only and grouping wrappers that do not change the runtime value. */
export function unwrapExpression(node: ESTree.Expression): ESTree.Expression {
  let current = node;
  while (
    current.type === "TSAsExpression" ||
    current.type === "TSSatisfiesExpression" ||
    current.type === "TSNonNullExpression" ||
    current.type === "ParenthesizedExpression" ||
    current.type === "ChainExpression"
  ) {
    current = current.expression;
  }
  return current;
}

/** Returns the text of a plain string literal, or undefined for every other expression. */
export function stringLiteralValue(node: ESTree.Expression): string | undefined {
  const expression = unwrapExpression(node);
  if (expression.type === "Literal" && QUOTES.has(expression.raw?.charAt(0) ?? "")) {
    return String(expression.value);
  }
  if (expression.type === "TemplateLiteral" && expression.expressions.length === 0) {
    return expression.quasis[0]?.value.cooked ?? undefined;
  }
  return undefined;
}

/**
 * Returns the tag names that an expression can produce when every branch is a string literal.
 * `cond ? "Created" : "Exists"` gives both names.
 */
export function literalTagNames(node: ESTree.Expression): Array<string> | undefined {
  const expression = unwrapExpression(node);
  if (expression.type === "ConditionalExpression") {
    const whenTrue = literalTagNames(expression.consequent);
    const whenFalse = literalTagNames(expression.alternate);
    if (whenTrue === undefined || whenFalse === undefined) return undefined;
    return [...whenTrue, ...whenFalse];
  }
  const value = stringLiteralValue(expression);
  return value === undefined ? undefined : [value];
}

/** Reports whether a property key names `_tag`, in identifier or string form. */
export function isTagKey(key: ESTree.PropertyKey, computed: boolean): boolean {
  if (key.type === "Identifier") return !computed && key.name === TAG_FIELD;
  if (key.type === "PrivateIdentifier") return false;
  return stringLiteralValue(key) === TAG_FIELD;
}

/** Returns the member expression when an expression reads `value._tag` or `value["_tag"]`. */
export function tagRead(node: ESTree.Expression): ESTree.MemberExpression | undefined {
  const expression = unwrapExpression(node);
  if (expression.type !== "MemberExpression") return undefined;
  return isTagKey(expression.property, expression.computed) ? expression : undefined;
}

/**
 * Reports whether a `_tag` read is on a nested `reason`, as in `error.reason._tag`.
 * Effect 4 handles these with `Effect.catchReason` and `Effect.catchReasons`.
 */
export function isReasonTagRead(read: ESTree.MemberExpression): boolean {
  const owner = unwrapExpression(read.object);
  if (owner.type !== "MemberExpression") return false;
  const key = owner.property;
  if (key.type === "Identifier") return !owner.computed && key.name === "reason";
  if (key.type === "PrivateIdentifier") return false;
  return stringLiteralValue(key) === "reason";
}
