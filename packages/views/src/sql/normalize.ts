import { Effect, Schema } from "effect";
import { StreamAddress } from "@streamsy/core";
import { encodeRowKey } from "../runtime/key.ts";
import type { DataFault } from "./data-fault.ts";
import type { StateChange } from "./engine.ts";
import type { Store } from "./store.ts";

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
) {
  const pending = new Map<string, StateChange>();
  const marker = `@snapshot/${sourceId}`;
  const markerKey = encodeRowKey(sourceId);
  let open = (yield* store.get("@snapshots", markerKey)) !== undefined;
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
      const value = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(event.value).pipe(
        Effect.mapError((error) => fault(key, error.message)),
      );

      pending.set(key, { kind: "upsert", sourceId, key, value });

      if (open) {
        seen.add(key);
        yield* store.snapshotMark(marker, key);
      }
    } else pending.set(key, { kind: "delete", sourceId, key });
  }

  return [...pending.values()];
});
