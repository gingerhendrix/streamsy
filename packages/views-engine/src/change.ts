import type { Change, JsonObject, RowKey } from "@streamsy/views-ir";
import { canonicalJson, encodeRowKey } from "./key.ts";

export const sameRow = (left: JsonObject, right: JsonObject): boolean =>
  canonicalJson(left) === canonicalJson(right);

export function transition(
  key: RowKey,
  before: JsonObject | undefined,
  after: JsonObject | undefined,
): Change<JsonObject> | undefined {
  if (before === undefined) return after === undefined ? undefined : { kind: "enter", key, after };
  if (after === undefined) return { kind: "exit", key, before };
  return sameRow(before, after) ? undefined : { kind: "update", key, before, after };
}

/** Coalesce only at an external materialization boundary. */
export function coalesceChanges(
  changes: readonly Change<JsonObject>[],
): readonly Change<JsonObject>[] {
  const touched = new Map<string, { key: RowKey; before?: JsonObject; after?: JsonObject }>();
  for (const change of changes) {
    const encoded = encodeRowKey(change.key);
    const item = touched.get(encoded) ?? { key: change.key };
    if (!touched.has(encoded)) {
      item.before = change.kind === "enter" ? undefined : change.before;
      touched.set(encoded, item);
    }
    item.after = change.kind === "exit" ? undefined : change.after;
  }
  return [...touched.values()].flatMap((item) => {
    const result = transition(item.key, item.before, item.after);
    return result === undefined ? [] : [result];
  });
}
