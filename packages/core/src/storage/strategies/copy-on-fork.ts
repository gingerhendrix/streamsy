import type { LineagePolicy } from "./lineage-store.ts";

export function copyOnForkReclaim(): LineagePolicy {
  return {
    async countDependents(): Promise<number> {
      return 0;
    },
    async addEdge(): Promise<void> {},
    async dropEdge(): Promise<void> {},
  };
}
