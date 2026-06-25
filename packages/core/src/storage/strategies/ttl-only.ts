import type { LineagePolicy } from "./lineage-store.ts";

export function ttlOnlyReclaim(): LineagePolicy {
  return {
    async countDependents(): Promise<number> {
      return 1;
    },
    async addEdge(): Promise<void> {},
    async dropEdge(): Promise<void> {},
  };
}
