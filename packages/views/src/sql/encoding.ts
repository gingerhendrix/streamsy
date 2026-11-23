import type { JsonValue, SortTerm } from "../ir/contracts.ts";
import { evaluate, type ExpressionScopes } from "../runtime/expression.ts";
import { encodeRowKey } from "../runtime/key.ts";
import type { Entry } from "./sqlite.ts";

/* oxlint-disable anti-slop/no-runtime-typeof -- Ordered JSON scalars are validated at this encoding boundary. */
/** Self-delimiting hex keys preserve scalar order under SQLite BINARY collation. */
export function scalarKey(value: JsonValue): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setFloat64(0, value === 0 ? 0 : value, false);

    if ((bytes[0] ?? 0) & 128) {
      for (let i = 0; i < 8; i++) bytes[i] = 255 - (bytes[i] ?? 0);
    } else bytes[0] = (bytes[0] ?? 0) ^ 128;

    return `1${bytes.toHex()}`;
  }

  if (typeof value === "string") {
    let result = "2";

    for (let i = 0; i < value.length; i++)
      result += (value.charCodeAt(i) + 1).toString(16).padStart(5, "0");

    return `${result}00000`;
  }

  if (typeof value === "boolean") return value ? "31" : "30";
  throw new TypeError("order requires a finite number, string, or Boolean");
}

export function orderKey(
  entry: Entry,
  terms: readonly SortTerm[],
  parameter?: ExpressionScopes["parameter"],
): string {
  let result = "";

  for (const term of terms) {
    const expression = term.expression;

    const value = scalarKey(
      expression.kind === "reference" && expression.scope === "key" && expression.path.length === 0
        ? encodeRowKey(entry.key)
        : evaluate(expression, { row: entry.row, key: entry.key, parameter }),
    );

    result += term.direction === "ascending" ? value : invert(value);
  }

  return result;
}

function invert(value: string): string {
  let result = "";

  for (const digit of value) result += (15 - Number.parseInt(digit, 16)).toString(16);

  return result;
}
