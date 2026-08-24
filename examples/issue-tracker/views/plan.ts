/**
 * Stable plan encoding and hashing.
 *
 * A plan is the thing two hosts must agree on, so its encoding is canonical:
 * object keys are emitted in sorted order, so a plan built by a refactored
 * constructor with the same meaning encodes to the same bytes and hashes to the
 * same value.
 *
 * The hash is FNV-1a over that encoding. It is deliberately *not* a WebCrypto
 * digest: `crypto.subtle.digest` is async, and an async plan hash would force
 * every inert declaration to become a promise. This is a change-detection and
 * inspection identity, not a security boundary.
 */
import { isJsonObject, type JsonValue, type RelationPlan } from "./contracts.ts";

/** Canonical JSON for a plan: sorted keys, no incidental whitespace. */
export function encodePlan(plan: RelationPlan): string {
  // SAFETY: every field of every `RelationNode` is a JSON scalar, a readonly
  // array of nodes, or an `Expression` — itself a closed union of JSON values.
  // A plan is a `JsonValue` by construction; the declaration vocabulary admits
  // nothing else.
  // oxlint-disable-next-line anti-slop/no-chained-type-assertions, typescript/no-unsafe-type-assertion -- Justified immediately above: the plan grammar is a JSON subtype.
  const value = plan as unknown as JsonValue;
  return canonical(value);
}

/** Stable 32-bit identity of a plan, rendered as 8 lowercase hex digits. */
export function planHash(plan: RelationPlan): string {
  let hash = 0x811c9dc5;
  const encoded = encodePlan(plan);
  for (let index = 0; index < encoded.length; index += 1) {
    hash ^= encoded.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

function canonical(value: JsonValue): string {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- `JsonValue` is a closed union with no discriminator; separating its scalar arm from its object arms is exactly the parse this encoder performs.
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (!isJsonObject(value)) return `[${value.map(canonical).join(",")}]`;
  const entries = Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonical(value[key] ?? null)}`);
  return `{${entries.join(",")}}`;
}
