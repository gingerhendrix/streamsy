import type { OutputChange } from "../../src/sql/engine.ts";
import { canonicalJson, encodeRowKey } from "../../src/runtime/key.ts";

export function identity(view: string, key: Parameters<typeof encodeRowKey>[0]): string {
  return `${view}/${encodeRowKey(key)}`;
}

export function materialize(rows: Map<string, string>, changes: readonly OutputChange[]): void {
  for (const c of changes) {
    const id = identity(c.view, c.key);

    if (c.kind === "delete") rows.delete(id);
    else rows.set(id, canonicalJson(c.value));
  }
}
