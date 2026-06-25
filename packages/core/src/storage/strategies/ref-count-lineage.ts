import type { StreamId } from "../../types/storage.ts";
import type { LineagePolicy, LineageStore } from "./lineage-store.ts";

export function refCountLineage(store: LineageStore): LineagePolicy {
  return {
    async countDependents(parent: StreamId): Promise<number> {
      return store.countDependents ? store.countDependents(parent) : 0;
    },
    async addEdge(parent: StreamId, child: StreamId): Promise<void> {
      await store.addEdge?.(parent, child);
    },
    async dropEdge(parent: StreamId, child: StreamId): Promise<void> {
      await store.dropEdge?.(parent, child);
    },
  };
}
