import type { JsonValue, RowKey, RowKeyPart } from "../ir/contracts.ts";
import { Array as Arr, Predicate, Record as Rec } from "effect";

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion -- The canonical key codec is the runtime boundary that validates discriminated JSON scalar representations. */

/** Collision-safe structural identity for public scalar and composite row keys. */
export function encodeRowKey(key: RowKey): string {
  return Arr.isArray<typeof key>(key)
    ? `a:${Arr.join(Arr.map(key, encodePart), "")}`
    : encodePart(key);
}

/** Order encoded row keys by code point, matching lexicographic UTF-8 byte order. */
export function compareRowKeys(left: string, right: string): number {
  let i = 0,
    j = 0;

  while (i < left.length && j < right.length) {
    const a = left.codePointAt(i)!,
      b = right.codePointAt(j)!;

    const order = utf8CodePoint(a) - utf8CodePoint(b);

    if (order !== 0) return order;
    i += a > 0xffff ? 2 : 1;
    j += b > 0xffff ? 2 : 1;
  }

  return left.length - i - (right.length - j);
}

function utf8CodePoint(point: number): number {
  // codePointAt only returns a surrogate code point for an unpaired code unit.
  return point >= 0xd800 && point <= 0xdfff ? 0xfffd : point;
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

  if (Arr.isArray<typeof value>(value) && Arr.every(value, isPart)) return value;
  throw new TypeError("expression did not produce a scalar or composite row key");
}

function isPart(value: JsonValue): value is RowKeyPart {
  return (
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

/* oxlint-enable anti-slop/no-runtime-typeof, anti-slop/require-safety-comment-for-type-assertion, typescript/no-unsafe-type-assertion */

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters -- Canonical encoding is the runtime parser and validation boundary for values presented as plans. */
/** Canonical JSON identity for plans, arrangements, expression values and rows. */
export function canonicalJson(value: unknown): string {
  return encode(value, new Set<object>(), "$");
}

function encode(value: unknown, ancestors: Set<object>, path: string): string {
  if (value === null) return "null";

  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);

  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`${path} contains a non-finite number`);

    return JSON.stringify(value);
  }

  if (typeof value !== "object") throw new TypeError(`${path} contains non-JSON ${typeof value}`);

  if (ancestors.has(value)) throw new TypeError(`${path} contains a cycle`);
  ancestors.add(value);

  try {
    if (Arr.isArray(value)) {
      return `[${Arr.join(
        Arr.map(value, (child, index) => encode(child, ancestors, `${path}[${index}]`)),
        ",",
      )}]`;
    }

    if (
      !Predicate.isObject(value) ||
      (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
    ) {
      throw new TypeError(`${path} contains a non-plain object`);
    }

    const entries = Rec.toEntries(value);
    entries.sort(([left], [right]) => left.localeCompare(right));

    return `{${Arr.join(
      Arr.map(
        entries,
        ([key, child]) => `${JSON.stringify(key)}:${encode(child, ancestors, `${path}.${key}`)}`,
      ),
      ",",
    )}}`;
  } finally {
    ancestors.delete(value);
  }
}
/* oxlint-enable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters */
