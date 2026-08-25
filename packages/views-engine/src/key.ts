import type { JsonValue, RowKey, RowKeyPart } from "@streamsy/views-ir";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion -- The canonical key codec is the runtime boundary that validates discriminated JSON scalar representations. */

/** Collision-safe structural identity for public scalar and composite row keys. */
export function encodeRowKey(key: RowKey): string {
  return Array.isArray(key) ? `a:${key.map(encodePart).join("")}` : encodePart(key as RowKeyPart);
}

function encodePart(part: RowKeyPart): string {
  if (typeof part === "string") return `s${part.length}:${part}`;
  if (typeof part === "boolean") return part ? "b1:" : "b0:";
  if (!Number.isFinite(part)) throw new TypeError("row keys require finite numbers");
  const value = Object.is(part, -0) ? "0" : String(part);
  return `n${value.length}:${value}`;
}

export function asRowKey(value: JsonValue): RowKey {
  if (isPart(value)) return value;
  if (Array.isArray(value) && value.every(isPart)) return value;
  throw new TypeError("expression did not produce a scalar or composite row key");
}

function isPart(value: JsonValue): value is RowKeyPart {
  return (
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

/** Canonical JSON identity used for expression values, partitions, and rows. */
export function canonicalJson(value: JsonValue): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  return `{${Object.entries(value)
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(([key, child]) => `${JSON.stringify(key)}:${canonicalJson(child)}`)
    .join(",")}}`;
}
