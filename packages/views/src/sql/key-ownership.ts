import { Array, Effect, Predicate } from "effect";
import type { KeyNode } from "../ir/contracts.ts";
import { evaluate, type ExpressionScopes } from "../runtime/expression.ts";
import { asRowKey, encodeRowKey } from "../runtime/key.ts";
import { checkData, type DataFault } from "./data-fault.ts";
import { difference, type Deltas } from "./runtime.ts";
import type { Entry, Store } from "../store.ts";

interface OwnedEntry extends Entry {
  readonly owner: string;
}

export const rekey = Effect.fnUntraced(function* (
  node: KeyNode,
  input: Deltas,
  store: Store,
  fault: DataFault,
  checked: boolean,
  routeArity: number | undefined,
  parameter: ExpressionScopes["parameter"],
) {
  const before = new Map<string, OwnedEntry>();
  const after = new Map<string, OwnedEntry>();

  for (const [owner, d] of input) {
    for (const [row, entries] of [
      [d.before, before],
      [d.after, after],
    ] as const) {
      if (row === undefined) continue;

      const evaluated = checkData(fault, d.key, () =>
        asRowKey(evaluate(node.key, { row, key: d.key, parameter })),
      );

      let key = evaluated;

      if (routeArity === 0) {
        key = String(evaluated);

        if (key === "") return yield* fault(key, "State key must not be empty");
      } else if (routeArity !== undefined) {
        if (!Array.isArray<typeof evaluated>(evaluated) || evaluated.length !== routeArity + 1)
          return yield* fault(d.key, "Invalid routed State key");
        const wireKey = evaluated[routeArity];

        if (wireKey === "" || (!Predicate.isString(wireKey) && !Predicate.isNumber(wireKey)))
          return yield* fault(d.key, "State key must be a non-empty string or a number");
        key = [...evaluated.slice(0, routeArity), String(wireKey)];
      }

      const id = encodeRowKey(key);

      if (entries.has(id)) return yield* fault(key, "duplicate output key");
      entries.set(id, { key, row, owner });
    }
  }

  if (checked && (before.size || after.size)) {
    const claims = new Map<string, string>();
    const releases: string[] = [];
    const current = yield* store.owners(node.id, [...after.keys()]);

    for (const [id, entry] of after) {
      const occupied = current.get(id);

      if (occupied !== undefined && occupied !== entry.owner && occupied !== before.get(id)?.owner)
        return yield* fault(entry.key, "duplicate output key");

      if (occupied !== entry.owner) claims.set(id, entry.owner);
    }

    for (const id of before.keys()) if (!after.has(id)) releases.push(id);

    yield* store.ownersRemove(node.id, releases);
    yield* store.ownersPut(node.id, claims);
  }

  return difference([...before.values()], [...after.values()], fault);
});
