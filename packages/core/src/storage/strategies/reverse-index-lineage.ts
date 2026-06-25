import type { StreamId } from "../../types/storage.ts";
import type { DependentsQuery, LineagePolicy } from "./lineage-store.ts";

export function reverseIndexLineage(query: DependentsQuery): LineagePolicy {
  return {
    countDependents(parent: StreamId): Promise<number> {
      return query(parent);
    },
    async addEdge(): Promise<void> {},
    async dropEdge(): Promise<void> {},
  };
}
