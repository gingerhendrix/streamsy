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

/** Effect combinators whose callback receives a value from the error channel. */
const ERROR_HANDLER = /^(?:catch|mapError|tapError|orElse)/u;

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

/** Reports whether a function is the callback of `catch*`, `mapError`, `tapError`, or `orElse`. */
export function isErrorHandler(fn: FunctionNode): boolean {
  const parent = fn.parent;
  if (parent.type !== "CallExpression") return false;
  const name = calleeName(parent);
  return name !== undefined && ERROR_HANDLER.test(name);
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
