/**
 * The inspectable expression vocabulary.
 *
 * `x.row.title` and `x.event.status` build {@link Expression} trees rather than
 * closures, so a declaration can be encoded, hashed, diffed and replayed. A
 * closure could do none of those things, which is why the drafted DSL keeps
 * selectors declarative and why this module is plain data plus a total
 * evaluator.
 */
import { isJsonObject, type Expression, type JsonValue, type ReferenceScope } from "./contracts.ts";

/**
 * A reference builder: readable as an {@link Expression}, extendable by
 * property access.
 *
 * The draft wrote a single untyped `x.row`. This version carries the row type,
 * so `x.event.projectId` is checked against the declared source schema at
 * compile time and a renamed field breaks the declaration rather than the
 * first fold that reads it.
 */
export type Reference<T> = Expression & {
  readonly [K in keyof T & string]-?: Reference<NonNullable<T[K]>>;
};

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-reflect-get, anti-slop/require-safety-comment-for-type-assertion -- A `Proxy` trap receives `string | symbol` from the language itself, and forwarding an own property is what `Reflect.get` is for; the builder's contract is stated in the SAFETY note below and proved by `test/declaration.test.ts`, which asserts the exact expression trees the selectors produce. */
const reference = <T>(scope: ReferenceScope, path: readonly string[]): Reference<T> => {
  const node: Expression = { kind: "reference", scope, path };
  // SAFETY: the trap answers every property that is not an own field of the
  // expression node with a nested reference, so the proxy satisfies
  // `Reference<T>` for every `K in keyof T`.
  return new Proxy(node, {
    get(target, property) {
      if (typeof property !== "string" || property in target) {
        return Reflect.get(target, property) as unknown;
      }
      return reference(scope, [...path, property]);
    },
  }) as Reference<T>;
};
/* oxlint-enable anti-slop/no-runtime-typeof, anti-slop/no-reflect-get, anti-slop/require-safety-comment-for-type-assertion */

/** A literal value, for the rare declaration that needs a constant. */
export const literal = (value: JsonValue): Expression => ({ kind: "literal", value });

export interface Selectors<Row, Event, State> {
  /** The current relation's row. */
  readonly row: Reference<Row>;
  /** The input fact a reducer is folding. */
  readonly event: Reference<Event>;
  /** The fold so far. */
  readonly state: Reference<State>;
  readonly literal: typeof literal;
}

/**
 * Selector roots for one relation.
 *
 * `const x = selectors<IssueEvent, IssueEvent, IssueRow>()` reproduces the
 * drafted `x.row` / `x.event` / `x.state` surface with the types filled in.
 */
export const selectors = <Row, Event = Row, State = Row>(): Selectors<Row, Event, State> => ({
  row: reference<Row>("row", []),
  event: reference<Event>("event", []),
  state: reference<State>("state", []),
  literal,
});

/** Everything an expression may read while it is evaluated. */
export interface Scopes {
  readonly row?: JsonValue;
  readonly event?: JsonValue;
  readonly state?: JsonValue;
}

/**
 * A path that does not resolve is a declaration bug, not an operational
 * outcome: the reducer's field set is fixed at declaration time and its inputs
 * are schema-decoded before they arrive here.
 */
export class ExpressionEvaluationError extends TypeError {
  constructor(
    readonly expression: Expression,
    detail: string,
  ) {
    super(detail);
    this.name = "ExpressionEvaluationError";
  }
}

/* oxlint-disable anti-slop/no-runtime-typeof -- The evaluator walks `JsonValue`, a closed union with no discriminator. Establishing which arm a value is in is the parse this function exists to perform, and every failure is reported as a typed `ExpressionEvaluationError`. */

/** Evaluate one expression against the supplied scopes. Total and pure. */
export function evaluate(expression: Expression, scopes: Scopes): JsonValue {
  if (expression.kind === "literal") return expression.value;
  const root = scopes[expression.scope];
  if (root === undefined) {
    throw new ExpressionEvaluationError(expression, `scope ${expression.scope} is not in context`);
  }
  let current: JsonValue = root;
  for (const segment of expression.path) {
    if (!isJsonObject(current)) {
      throw new ExpressionEvaluationError(
        expression,
        `cannot read ${segment} from ${describe(current)}`,
      );
    }
    const next: JsonValue | undefined = current[segment];
    if (next === undefined) {
      throw new ExpressionEvaluationError(expression, `${segment} is absent`);
    }
    current = next;
  }
  return current;
}

/** Evaluate an expression that must produce a row key. */
export function evaluateKey(expression: Expression, scopes: Scopes): string {
  const value = evaluate(expression, scopes);
  if (typeof value !== "string" || value.length === 0) {
    throw new ExpressionEvaluationError(expression, "a row key must be a non-empty string");
  }
  return value;
}

/** Evaluate an expression that must produce a source order value. */
export function evaluateOrder(expression: Expression, scopes: Scopes): number {
  const value = evaluate(expression, scopes);
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ExpressionEvaluationError(expression, "a source order must be a finite number");
  }
  return value;
}

function describe(value: JsonValue): string {
  /* the caller has already failed; this only labels the value in the message */
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  return typeof value;
}
