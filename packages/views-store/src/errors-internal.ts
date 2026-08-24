import { ViewStateRestorePoison } from "./errors.ts";
import type { JsonValue } from "./contracts.ts";

export function decodeJson(table: string, identity: string, key: string, value: string): JsonValue {
  try {
    // SAFETY: JsonValue is the persistence grammar; callers perform domain-schema decoding above this boundary.
    return JSON.parse(value) as JsonValue;
  } catch (cause) {
    throw new ViewStateRestorePoison({
      table,
      identity,
      key,
      detail: cause instanceof Error ? cause.message : String(cause),
    });
  }
}
