/** The deliberately narrow evaluator for the two-node Slice 1 reducer plan. */
import { isJsonObject, type Expression, type JsonValue } from "@streamsy/views/ir";

export interface Scopes {
  readonly row?: JsonValue;
  readonly event?: JsonValue;
  readonly state?: JsonValue;
}

export class ExpressionEvaluationError extends TypeError {
  constructor(
    readonly expression: Expression,
    detail: string,
  ) {
    super(detail);
    this.name = "ExpressionEvaluationError";
  }
}

/** Evaluate only the literal/reference subset the accepted Slice 1 executes. */
export function evaluate(expression: Expression, scopes: Scopes): JsonValue {
  if (expression.kind === "literal") return expression.value;
  if (expression.kind !== "reference") {
    throw new ExpressionEvaluationError(
      expression,
      `Slice 1 cannot execute ${expression.kind} expressions`,
    );
  }
  if (expression.scope !== "row" && expression.scope !== "event" && expression.scope !== "state") {
    throw new ExpressionEvaluationError(
      expression,
      `Slice 1 cannot read ${expression.scope} expressions`,
    );
  }
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
    const next = current[segment];
    if (next === undefined) throw new ExpressionEvaluationError(expression, `${segment} is absent`);
    current = next;
  }
  return current;
}

export function evaluateKey(expression: Expression, scopes: Scopes): string {
  const value = evaluate(expression, scopes);
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- JsonValue is already parsed; Slice 1 deliberately narrows the expanded RowKey contract to its string-key subset.
  if (typeof value !== "string" || value.length === 0) {
    throw new ExpressionEvaluationError(expression, "a Slice 1 row key must be a non-empty string");
  }
  return value;
}

function describe(value: JsonValue): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- This only labels an already parsed JsonValue in an error message.
  return typeof value;
}
