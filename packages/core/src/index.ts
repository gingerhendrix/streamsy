/**
 * @streamsy/core
 *
 * Runtime-agnostic protocol and HTTP layers for the Durable Streams protocol.
 * Provides StreamProtocol, HttpHandler, and all type definitions.
 *
 * Storage backends are provided by separate packages
 * (e.g., @streamsy/storage-durable-object).
 */

// Core classes
export { StreamProtocol } from "./protocol.ts";
export { HttpHandler } from "./http.ts";

// Type exports
export type {
  StreamProtocolInterface,
  StorageFactory,
  CreateOptions,
  CreateResult,
  AppendOptions,
  AppendResult,
  ProducerOptions,
  ReadOptions,
  ReadResult,
  ReadLiveOptions,
  ReadLiveResult,
  MetadataResult,
  DeleteResult,
} from "./types/protocol.ts";

export type {
  StreamStorage,
  StreamMetadata,
  CreateStreamOptions,
  StorageReadResult,
  StorageReadLiveResult,
  StoredMessage,
  ProducerState,
} from "./types/storage.ts";
