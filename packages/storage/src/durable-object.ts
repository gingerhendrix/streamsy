import { provides, type StreamFamily, type StreamGroup, type Changes } from "@streamsy/core";
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
  }).pipe(Layer.provideMerge(layer(options)));

export function layer<const Keys extends ReadonlyArray<StreamFamily.AnyFamily>>(
  options: DurableObjectProtocolOptions,
  ...input: StreamGroup.NonEmptyInput<Keys>
): Layer.Layer<
  | Layer.Success<ReturnType<typeof storageLayer>>
  | StreamFamily.Identifiers<Keys>
  | Changes<StreamFamily.Identifiers<Keys>>,
  Layer.Error<ReturnType<typeof storageLayer>>,
  Layer.Services<ReturnType<typeof storageLayer>>
>;
export function layer(options: DurableObjectStorageOptions): ReturnType<typeof storageLayer>;
export function layer(
  options: DurableObjectProtocolOptions,
  ...input: StreamGroup.Input<ReadonlyArray<StreamFamily.AnyFamily>>
) {
  const storage = storageLayer(options);
  if (input.length === 0) return storage;
  const protocol = Protocol.layer({
    ...options,
    longPollTimeoutMs: options.longPollTimeoutMs ?? DEFAULT_LONG_POLL_TIMEOUT_MS,
  }).pipe(Layer.provideMerge(storage));
  return provides(protocol, ...input).pipe(Layer.provideMerge(storage));
}
