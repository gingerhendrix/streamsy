/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- This effect-free JSON boundary validates untrusted fields with primitive checks and own data descriptors. */
/**
 * Canonical encoding and the change-detection digests served routes need.
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
  if (typeof value === "number" && !Number.isFinite(value))
    throw new TypeError("Expected finite JSON number");
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

export const STATE_VERSION_HEADER = "x-streamsy-state-version";
export const STATE_CONTRACT_HEADER = "x-streamsy-state-contract";
export const STREAM_VERSION_HEADER = "x-streamsy-stream-version";
export const STREAM_CONTRACT_HEADER = "x-streamsy-stream-contract";
export const DOCUMENT_CONTRACT_HEADER = "x-streamsy-document-contract";
export const DURABLE_STATE_PROTOCOL = "https://durable-streams.dev/state-protocol/v1";
export type ReplayRecovery = "replay-from-start";
export type ResumeRejectedReason = "invalid-offset" | "history-unavailable" | "contract-changed";
export type PublicError =
  | {
      readonly _tag: "InvalidParams";
      readonly route: string;
      readonly parameter: string;
      readonly detail: string;
    }
  | {
      readonly _tag: "ProtocolVersionUnsupported";
      readonly route: string;
      readonly supported: 1;
      readonly received: string;
      readonly recovery: ReplayRecovery;
    }
  | {
      readonly _tag: "ResumeRejected";
      readonly route: string;
      readonly reason: ResumeRejectedReason;
      readonly recovery: ReplayRecovery;
    }
  | { readonly _tag: "ContractChanged"; readonly route: string; readonly recovery: "refetch" }
  | {
      readonly _tag: "TransportUnavailable" | "DocumentUnavailable" | "WireEncodeFailed";
      readonly route: string;
      readonly detail: string;
    };
export const PUBLIC_ERROR_TAGS = [
  "InvalidParams",
  "ProtocolVersionUnsupported",
  "ResumeRejected",
  "ContractChanged",
  "TransportUnavailable",
  "DocumentUnavailable",
  "WireEncodeFailed",
] as const;
export type PublicErrorTag = PublicError["_tag"];

/** The browser's JSON error boundary; unknown fields are discarded. */
export function decodePublicError(value: unknown): PublicError {
  if (value === null || !(value instanceof Object) || Array.isArray(value))
    throw new TypeError("Expected error object");
  const field = (key: string): unknown => Object.getOwnPropertyDescriptor(value, key)?.value;
  const string = (key: string): string => {
    const result = field(key);
    if (typeof result !== "string") throw new TypeError(`Expected string ${key}`);
    return result;
  };
  const _tag = string("_tag");
  const route = string("route");
  switch (_tag) {
    case "InvalidParams":
      return { _tag, route, parameter: string("parameter"), detail: string("detail") };
    case "ProtocolVersionUnsupported":
      if (field("supported") !== 1 || field("recovery") !== "replay-from-start")
        throw new TypeError("Invalid protocol recovery");
      return {
        _tag,
        route,
        supported: 1,
        received: string("received"),
        recovery: "replay-from-start",
      };
    case "ResumeRejected": {
      const reason = field("reason");
      if (
        (reason !== "invalid-offset" &&
          reason !== "history-unavailable" &&
          reason !== "contract-changed") ||
        field("recovery") !== "replay-from-start"
      )
        throw new TypeError("Invalid resume recovery");
      return { _tag, route, reason, recovery: "replay-from-start" };
    }
    case "ContractChanged":
      if (field("recovery") !== "refetch") throw new TypeError("Invalid document recovery");
      return { _tag, route, recovery: "refetch" };
    case "TransportUnavailable":
    case "DocumentUnavailable":
    case "WireEncodeFailed":
      return { _tag, route, detail: string("detail") };
    default:
      throw new TypeError("Unknown public error tag");
  }
}
