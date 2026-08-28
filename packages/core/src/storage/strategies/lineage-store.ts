import type { StorageDeleteResult } from "../../types/storage-adapter.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";

export interface LineageStore {
  getRecord(id: StreamId): Promise<StreamRecord | null>;
  purgeSelf(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean>;
  softDelete(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean>;
  addEdge?(parent: StreamId, child: StreamId): Promise<void>;
  dropEdge?(parent: StreamId, child: StreamId): Promise<void>;
  countDependents?(parent: StreamId): Promise<number>;
}

export interface LineagePolicy {
  countDependents(parent: StreamId): Promise<number>;
  addEdge(parent: StreamId, child: StreamId): Promise<void>;
  dropEdge(parent: StreamId, child: StreamId): Promise<void>;
}

export type DependentsQuery = (parent: StreamId) => Promise<number>;

export type ReclaimResult = StorageDeleteResult["status"];
