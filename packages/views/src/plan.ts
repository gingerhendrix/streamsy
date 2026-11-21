import type { RelationPlan } from "./ir/contracts.ts";
import { canonicalJson } from "./runtime/key.ts";
import { Record as Rec, Array as Arr } from "effect";

/** Keep circuit arrangement identities stable when view fields are reordered. */
export function orderPlanFields(plan: RelationPlan): RelationPlan {
  return {
    ...plan,
    nodes: Arr.map(plan.nodes, (node) => {
      if (node.kind === "project" && node.fields !== undefined)
        return { ...node, fields: orderedFields(node.fields) };

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

/** Identity of every named output plan, independent of declaration order. */
export function circuitPlanHash(plans: Readonly<Record<string, RelationPlan>>): string {
  return hashText(canonicalJson(plans));
}

function hashText(text: string): string {
  let hash = 0x811c9dc5;

  for (const byte of new TextEncoder().encode(text)) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }

  return hash.toString(16).padStart(8, "0");
}
