/**
 * Canonical encoding and the change-detection digests these sinks need.
 *
 * A contract fingerprint and a document ETag answer the same question — "is
 * this still the thing you were holding?" — so both are computed from one
 * canonical encoding. Object keys are sorted and array order is preserved, so
 * two values that differ only in property order encode identically and cannot
 * invalidate a consumer's cache for no reason.
 *
 * These are change-detection identities, not security digests.
 */

export type CanonicalValue =
  | string
  | number
  | boolean
  | null
  | readonly CanonicalValue[]
  | { readonly [key: string]: CanonicalValue };

/** Encode a value with object keys sorted, recursively. Array order is preserved. */
export function canonicalJson(value: CanonicalValue): string {
  if (isCanonicalArray(value)) return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  if (value === null) return "null";
  if (value instanceof Object) return canonicalObject(value);
  return JSON.stringify(value);
}

/**
 * A written predicate rather than a bare `Array.isArray` call, because only a
 * predicate narrows the non-array branch of this union to the record type.
 */
function isCanonicalArray(value: CanonicalValue): value is readonly CanonicalValue[] {
  return Array.isArray(value);
}

function canonicalObject(value: { readonly [key: string]: CanonicalValue }): string {
  const entries: string[] = [];
  for (const key of Object.keys(value).toSorted(compareKeys)) {
    const member = value[key];
    if (member === undefined) continue;
    entries.push(`${JSON.stringify(key)}:${canonicalJson(member)}`);
  }
  return `{${entries.join(",")}}`;
}

function compareKeys(left: string, right: string): number {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

/** Eight lowercase hex characters of FNV-1a over the canonical encoding's UTF-16 code units. */
export function contractFingerprint(value: CanonicalValue): string {
  return hex(fnv1a(canonicalJson(value), 0x81_1c_9d_c5));
}

/**
 * Sixteen lowercase hex characters, as a strong entity tag body.
 *
 * Two FNV-1a passes with different offset bases are concatenated, because a
 * cache validator that collides serves a stale document: eight hex characters
 * are enough to notice that a contract changed, but not enough to key a
 * consumer's cache on.
 */
export function documentEtag(canonical: string): string {
  return `"${hex(fnv1a(canonical, 0x81_1c_9d_c5))}${hex(fnv1a(canonical, 0x01_00_01_93))}"`;
}

function fnv1a(input: string, seed: number): number {
  let hash = seed;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01_00_01_93);
  }
  return hash >>> 0;
}

function hex(value: number): string {
  return value.toString(16).padStart(8, "0");
}
