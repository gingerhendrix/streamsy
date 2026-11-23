import { Array as Arr, Effect, Predicate, Record, Schema } from "effect";
import { StreamAddress } from "@streamsy/core";
import { encodeRowKey } from "../runtime/key.ts";
import type { DataFault } from "./data-fault.ts";
import type { StateChange } from "./engine.ts";
import type { Store } from "./sqlite.ts";

/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns, anti-slop/no-known-value-widening -- Keep unvalidated State values intact for the JSON decoder. */
function omitUndefinedMembers(value: unknown): unknown {
  const copies = new WeakMap<object, object>();
  const pending: (() => void)[] = [];

  const copy = (input: unknown): unknown => {
    if (!Predicate.isObjectOrArray(input)) return input;
    const existing = copies.get(input);

    if (existing !== undefined) return existing;

    if (Arr.isArray(input)) {
      const items: unknown[] = [];
      copies.set(input, items);
      pending.push(() => {
        for (let index = 0; index < input.length; index++) items.push(copy(input[index]));
      });

      return items;
    }

    const prototype = Reflect.getPrototypeOf(input);

    if (
      prototype !== null &&
      prototype !== Object.prototype &&
      Reflect.getPrototypeOf(prototype) !== null
    )
      return input;

    const row = {};
    copies.set(input, row);
    pending.push(() => {
      for (const key of Record.keys(input)) {
        const member = input[key];

        if (member !== undefined)
          Object.defineProperty(row, key, {
            value: copy(member),
            enumerable: true,
            configurable: true,
            writable: true,
          });
      }
    });

    if (prototype !== Object.prototype) Object.setPrototypeOf(row, prototype);

    return row;
  };

  // Keep cycles and invalid array slots visible to the existing JSON decoder.
  const result = copy(value);

  for (let populate = pending.pop(); populate !== undefined; populate = pending.pop()) populate();

  return result;
}
/* oxlint-enable anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns, anti-slop/no-known-value-widening */

/** Normalize State controls without reading another projection's StateTable.
 * Ordinary units touch keys only; resets and snapshot-end may visit a relation.
 */
export const normalize = Effect.fn("View.normalize")(function* (
  store: Store,
  sourceId: string,
  relation: string,
  collection: string,
  fault: DataFault,
  events: readonly StreamAddress.CollectionsEvent<StreamAddress.Collections>[],
  snapshotOpen: boolean,
) {
  const pending = new Map<string, StateChange>();
  const marker = `@snapshot/${sourceId}`;
  const markerKey = encodeRowKey(sourceId);
  let open = snapshotOpen;
  const seen = new Set<string>();

  if (open) for (const entry of yield* store.lookup(marker, "")) seen.add(entry.row_key);

  const keys = Effect.gen(function* () {
    const result = new Set<string>();

    for (const entry of yield* store.rows(relation)) result.add(String(entry.key));

    for (const key of pending.keys()) result.add(key);

    return result;
  });

  const close = Effect.gen(function* () {
    open = false;
    seen.clear();
    yield* store.remove("@snapshots", [markerKey]);
    yield* store.clearIndex(marker);
  });

  for (const event of events) {
    if (StreamAddress.isControl(event)) {
      if (event.headers.control === "reset") {
        for (const key of yield* keys) pending.set(key, { kind: "delete", sourceId, key });
        yield* close;
      } else if (event.headers.control === "snapshot-start") {
        seen.clear();
        open = true;
        yield* store.clearIndex(marker);
        yield* store.put("@snapshots", [{ key: sourceId, row: {} }]);
      } else if (open) {
        for (const key of yield* keys)
          if (!seen.has(key)) pending.set(key, { kind: "delete", sourceId, key });
        yield* close;
      }

      continue;
    }

    if (event.type !== collection) continue;
    const key = event.key;

    if (StreamAddress.isUpsert(event)) {
      const value = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(
        omitUndefinedMembers(event.value),
      ).pipe(Effect.mapError((error) => fault(key, error.message)));

      pending.set(key, { kind: "upsert", sourceId, key, value });

      if (open) {
        seen.add(key);
        yield* store.snapshotMark(marker, key);
      }
    } else pending.set(key, { kind: "delete", sourceId, key });
  }

  return [...pending.values()];
});
