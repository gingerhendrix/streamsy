/**
 * @streamsy/storage-memory
 *
 * In-memory storage backend for the Durable Streams protocol.
 * Implements the StreamStorage interface from @streamsy/core
 * using plain JavaScript data structures.
 *
 * Suitable for development, testing, and ephemeral use cases.
 * Works on any JavaScript runtime (Node, Bun, Deno, browsers).
 */

export { MemoryStreamStorage } from "./storage.ts";
export type { MemoryStreamStorageOptions } from "./storage.ts";

import {
  MemoryStreamStorage,
  type MemoryStreamStorageOptions,
} from "./storage.ts";
import type { StorageFactory } from "@streamsy/core";

/**
 * Creates a StorageFactory backed by in-memory storage.
 * Each unique streamId gets its own MemoryStreamStorage instance.
 *
 * Usage:
 * ```typescript
 * import { StreamProtocol, HttpHandler } from "@streamsy/core";
 * import { createMemoryStorageFactory } from "@streamsy/storage-memory";
 *
 * const storage = createMemoryStorageFactory();
 * const protocol = new StreamProtocol(storage);
 * const handler = new HttpHandler({ protocol });
 * ```
 */
export function createMemoryStorageFactory(
  options: MemoryStreamStorageOptions = {},
): StorageFactory {
  const stores = new Map<string, MemoryStreamStorage>();
  return (streamId: string) => {
    let store = stores.get(streamId);
    if (!store) {
      store = new MemoryStreamStorage(options);
      stores.set(streamId, store);
    }
    return store;
  };
}

// Re-export core types that users of this package will need
export type {
  StreamStorage,
  StorageFactory,
  StreamMetadata,
  CreateStreamOptions,
  StorageReadResult,
  StorageReadLiveResult,
  StoredMessage,
} from "@streamsy/core";
