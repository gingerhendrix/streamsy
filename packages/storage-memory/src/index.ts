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
 * Instances are kept in a shared registry so cross-stream lookups
 * (forks resolving their source) work transparently.
 */
export function createMemoryStorageFactory(
  options: MemoryStreamStorageOptions = {},
): StorageFactory {
  const stores = new Map<string, MemoryStreamStorage>();
  const factory: StorageFactory = (streamId: string) => {
    let store = stores.get(streamId);
    if (!store) {
      store = new MemoryStreamStorage({
        ...options,
        onPurge: () => {
          stores.delete(streamId);
          return options.onPurge?.();
        },
      });
      stores.set(streamId, store);
    }
    return store;
  };
  return factory;
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
