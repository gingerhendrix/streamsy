/** @streamsy/storage-memory */
export { MemoryStreamStore } from "./storage.ts";
export { createMemoryStreamFactory } from "./factory.ts";
export type { MemoryStreamFactoryOptions } from "./factory.ts";
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
