/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters -- decodeKey validates JSON.parse output recursively at the persistence boundary; the closed JSON grammar has no discriminator. */
import type { JsonValue, RowKey } from "./contracts.ts";

export function canonicalJson(value: JsonValue): string {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- JsonValue is a closed parsed union; this separates its scalar arm.
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (isObject(value))
    return `{${Object.keys(value)
      .toSorted()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key]!)}`)
      .join(",")}}`;
  throw new TypeError("unsupported JSON value");
}
export const encodeKey = (key: RowKey): string => canonicalJson(key);
export function decodeKey(encoded: string): RowKey {
  const parsed: unknown = JSON.parse(encoded);
  if (isRowKey(parsed)) return parsed;
  throw new TypeError("encoded value is not a row key");
}
export function compareKeys(left: RowKey, right: RowKey): number {
  return encodeKey(left).localeCompare(encodeKey(right));
}

function isObject(value: JsonValue): value is { readonly [key: string]: JsonValue } {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- JsonValue is already parsed; this narrows its object arm.
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isRowKey(value: unknown): value is RowKey {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  return Array.isArray(value) && value.every(isJsonValue);
}
function isJsonValue(value: unknown): value is JsonValue {
  if (isRowKey(value)) return true;
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  return Object.values(value).every(isJsonValue);
}
