import { SqliteClient } from "@effect/sql-sqlite-do";
import { Layer } from "effect";
import { Protocol } from "@streamsy/core";
import { sharedSqlClientLayer } from "./boundary.ts";
import { layerWithDurableObjectTransactions, type SqlStorageOptions } from "./storage.ts";

export interface DurableObjectStorageOptions extends SqlStorageOptions {
  /** Full DurableObjectStorage is required because Storage mutations are transactional. */
  readonly client: Parameters<typeof SqliteClient.make>[0] & {
    readonly storage: NonNullable<Parameters<typeof SqliteClient.make>[0]["storage"]>;
  };
}

export const DEFAULT_LONG_POLL_TIMEOUT_MS = 25_000;

export interface DurableObjectProtocolOptions
  extends DurableObjectStorageOptions, Protocol.ProtocolOptions {}

/** Official local/host Durable Object SQLite layer with commit-boundary support. */
export const layer = (options: DurableObjectStorageOptions) => {
  const clientLayer = Layer.effectContext(sharedSqlClientLayer(SqliteClient.make(options.client)));
  return layerWithDurableObjectTransactions(options).pipe(Layer.provideMerge(clientLayer));
};

/** Protocol over the official Durable Object SQLite layer. */
export const layerProtocol = (options: DurableObjectProtocolOptions) =>
  Protocol.layer({
    ...options,
    longPollTimeoutMs: options.longPollTimeoutMs ?? DEFAULT_LONG_POLL_TIMEOUT_MS,
  }).pipe(Layer.provideMerge(layer(options)));
