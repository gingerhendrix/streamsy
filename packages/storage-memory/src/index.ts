/** @streamsy/storage-memory */
export { MemoryStreamStore } from "./storage.ts";
import { MemoryStreamStore } from "./storage.ts";
import type { StreamStoreAdapter } from "@streamsy/core";

export function createMemoryStreamStore(): StreamStoreAdapter {
  return new MemoryStreamStore();
}

export type {
  StreamStoreAdapter,
  StreamRecord,
  StoredMessage,
  ProducerState,
} from "@streamsy/core";
