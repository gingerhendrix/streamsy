import type { SqlRelationPlan } from "./ir/contracts.ts";
import { Predicate, Record as Rec, Array as Arr } from "effect";

/** Keep circuit arrangement identities stable when view fields are reordered. */
export function orderPlanFields(plan: SqlRelationPlan): SqlRelationPlan {
  return {
    ...plan,
    nodes: Arr.map(plan.nodes, (node) => {
      if (node.kind === "project") return { ...node, fields: orderedFields(node.fields) };

      if (node.kind === "grouped-aggregate")
        return {
          ...node,
          groupBy: orderedFields(node.groupBy),
          aggregates: orderedFields(node.aggregates),
        };

      return node;
    }),
  };
}

function orderedFields<A>(fields: Readonly<Record<string, A>>): Record<string, A> {
  const entries = Rec.toEntries(fields);
  entries.sort(([left], [right]) => left.localeCompare(right));

  return Rec.fromEntries(entries);
}

/** Canonical JSON with lexicographically sorted object keys and preserved array order. */
export function encodePlan(plan: SqlRelationPlan): string {
  return canonical(plan, new Set<object>(), "$plan");
}

/** Eight-lowercase-hex FNV-1a identity over the canonical UTF-8 bytes. */
export function planHash(plan: SqlRelationPlan): string {
  return hashText(encodePlan(plan));
}

/** Identity of every named output plan, independent of declaration order. */
export function circuitPlanHash(plans: Readonly<Record<string, SqlRelationPlan>>): string {
  return hashText(canonical(plans, new Set<object>(), "$circuit"));
}

function hashText(text: string): string {
  let hash = 0x811c9dc5;

  for (const byte of new TextEncoder().encode(text)) {
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
    if (Arr.isArray(value)) {
      return `[${Arr.join(
        Arr.map(value, (child, index) => canonical(child, ancestors, `${path}[${index}]`)),
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
        ([key, child]) => `${JSON.stringify(key)}:${canonical(child, ancestors, `${path}.${key}`)}`,
      ),
      ",",
    )}}`;
  } finally {
    ancestors.delete(value);
  }
}
/* oxlint-enable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters */
