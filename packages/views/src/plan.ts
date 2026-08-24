import type { RelationPlan } from "@streamsy/views-ir";

/** Canonical JSON with lexicographically sorted object keys and preserved array order. */
export function encodePlan(plan: RelationPlan): string {
  return canonical(plan, new Set<object>(), "$plan");
}

/** Eight-lowercase-hex FNV-1a identity over the canonical UTF-8 bytes. */
export function planHash(plan: RelationPlan): string {
  let hash = 0x811c9dc5;
  for (const byte of new TextEncoder().encode(encodePlan(plan))) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters -- Canonical encoding is the runtime parser and validation boundary for values presented as plans. */
function canonical(value: unknown, ancestors: Set<object>, path: string): string {
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
    if (Array.isArray(value)) {
      return `[${value.map((child, index) => canonical(child, ancestors, `${path}[${index}]`)).join(",")}]`;
    }
    if (
      Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null
    ) {
      throw new TypeError(`${path} contains a non-plain object`);
    }
    return `{${Object.entries(value)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(
        ([key, child]) => `${JSON.stringify(key)}:${canonical(child, ancestors, `${path}.${key}`)}`,
      )
      .join(",")}}`;
  } finally {
    ancestors.delete(value);
  }
}
/* oxlint-enable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters */
