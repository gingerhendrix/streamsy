import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Layer } from "effect";
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
const storageLayer = (options: DurableObjectStorageOptions) => {
  const clientLayer = Layer.effectContext(sharedSqlClientLayer(SqliteClient.make(options.client)));
  return layerWithDurableObjectTransactions(options).pipe(Layer.provideMerge(clientLayer));
};

/** Protocol over the official Durable Object SQLite layer. */
export const layerProtocol = (options: DurableObjectProtocolOptions) =>
  Protocol.layer({
    ...options,
    longPollTimeoutMs: options.longPollTimeoutMs ?? DEFAULT_LONG_POLL_TIMEOUT_MS,
  }).pipe(Layer.provideMerge(storageLayer(options)));

export function layer<const Keys extends ReadonlyArray<Streamsy.Any>>(
  group: Streamsy.Group<Keys>,
  options: DurableObjectProtocolOptions,
): Layer.Layer<
  | Layer.Success<ReturnType<typeof storageLayer>>
  | Streamsy.Services<Streamsy.Group<Keys>>
  | Streamsy.ChangeFeed<Streamsy.Services<Streamsy.Group<Keys>>>,
  Layer.Error<ReturnType<typeof storageLayer>>,
  Layer.Services<ReturnType<typeof storageLayer>>
>;
export function layer(group: Streamsy.Group, options: DurableObjectProtocolOptions) {
  const storage = storageLayer(options);
  const protocol = Protocol.layer({
    ...options,
    longPollTimeoutMs: options.longPollTimeoutMs ?? DEFAULT_LONG_POLL_TIMEOUT_MS,
  }).pipe(Layer.provideMerge(storage));
  return Protocol.bind(protocol, group).pipe(Layer.provideMerge(storage));
}
