import type { ESTree } from "@oxlint/plugins";

/** Guards that Effect already exports for the tags of its own data types. */
const BUILT_IN_GUARDS = new Map([
  ["Some", "Option.isSome"],
  ["None", "Option.isNone"],
  ["Success", "Exit.isSuccess or Result.isSuccess"],
  ["Failure", "Exit.isFailure or Result.isFailure"],
  ["Fail", "Cause.isFailReason"],
  ["Die", "Cause.isDieReason"],
  ["Interrupt", "Cause.isInterruptReason"],
]);

/** Effect combinators whose callback recovers from a value in the error channel. */
const RECOVERY_HANDLER = /^(?:catch|orElse)/u;

/** Effect combinators whose callback maps or observes the error channel without recovery. */
const MAPPING_HANDLER = /^(?:mapError|tapError)/u;

/** Match combinators whose object argument is a pattern, not a constructed value. */
const MATCH_PATTERNS = new Set(["when", "whenOr", "whenAnd", "not"]);

/** How an error-channel callback treats the error it receives. */
export type ErrorHandlerKind = "recovery" | "mapping";

/** Retry and repeat options whose value is a predicate over the error or output. */
const PREDICATE_OPTIONS = new Set(["while", "until"]);

type FunctionNode = ESTree.ArrowFunctionExpression | ESTree.Function;

function isFunctionNode(node: ESTree.Node): node is FunctionNode {
  return (
    node.type === "ArrowFunctionExpression" ||
    node.type === "FunctionExpression" ||
    node.type === "FunctionDeclaration"
  );
}

/** Returns the Effect guard for a built-in tag, such as `Option.isSome` for "Some". */
export function builtInGuard(tag: string): string | undefined {
  return BUILT_IN_GUARDS.get(tag);
}

/** Returns the nearest function that contains a node. */
export function enclosingFunction(node: ESTree.Node): FunctionNode | undefined {
  let current: ESTree.Node | null = node.parent;
  while (current !== null && current.type !== "Program") {
    if (isFunctionNode(current)) return current;
    current = current.parent;
  }
  return undefined;
}

/** Returns the called name for `name(...)` and `object.name(...)`. */
export function calleeName(call: ESTree.CallExpression): string | undefined {
  const callee = call.callee;
  if (callee.type === "Identifier") return callee.name;
  if (callee.type === "MemberExpression" && callee.property.type === "Identifier") {
    return callee.property.name;
  }
  return undefined;
}

/**
 * Classifies the callback of an error-channel combinator. `catch*` and `orElse` recover from the
 * error. `mapError` and `tapError` only map or observe it.
 */
export function errorHandlerKind(fn: FunctionNode): ErrorHandlerKind | undefined {
  const parent = fn.parent;
  if (parent.type !== "CallExpression") return undefined;
  const name = calleeName(parent);
  if (name === undefined) return undefined;
  if (RECOVERY_HANDLER.test(name)) return "recovery";
  if (MAPPING_HANDLER.test(name)) return "mapping";
  return undefined;
}

/** Classifies the nearest function around a node as an error-channel callback. */
export function enclosingErrorHandlerKind(node: ESTree.Node): ErrorHandlerKind | undefined {
  const fn = enclosingFunction(node);
  return fn === undefined ? undefined : errorHandlerKind(fn);
}

/**
 * Returns the call that receives an object literal as an argument. The search climbs through
 * enclosing object literals, properties, and arrays, so a nested value finds the outer call.
 */
export function argumentCall(node: ESTree.ObjectExpression): ESTree.CallExpression | undefined {
  let current: ESTree.Node = node;
  while (
    current.parent.type === "Property" ||
    current.parent.type === "ObjectExpression" ||
    current.parent.type === "ArrayExpression"
  ) {
    current = current.parent;
  }
  const parent = current.parent;
  if (parent.type !== "CallExpression" || parent.callee === current) return undefined;
  return parent;
}

/** Reports whether a call is `Match.when`, `Match.whenOr`, `Match.whenAnd`, or `Match.not`. */
export function isMatchPatternCall(call: ESTree.CallExpression): boolean {
  const callee = call.callee;
  if (callee.type !== "MemberExpression" || callee.computed) return false;
  if (callee.object.type !== "Identifier" || callee.object.name !== "Match") return false;
  return callee.property.type === "Identifier" && MATCH_PATTERNS.has(callee.property.name);
}

/** Reports whether a function is the `while` or `until` option of a retry or repeat. */
export function isRetryPredicate(fn: FunctionNode): boolean {
  const parent = fn.parent;
  if (parent.type !== "Property" || parent.value !== fn) return false;
  return parent.key.type === "Identifier" && PREDICATE_OPTIONS.has(parent.key.name);
}

/**
 * Reports whether a function is exactly `(value) => value._tag === "Tag"`.
 * `Predicate.isTagged("Tag")` replaces that function without other changes.
 */
export function isTagPredicateLambda(fn: FunctionNode, comparison: ESTree.Node): boolean {
  return fn.type === "ArrowFunctionExpression" && fn.expression && fn.body === comparison;
}
