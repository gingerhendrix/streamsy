import { Layer } from "effect";
import * as Protocol from "../../protocol/layer.ts";
import { bind, type Any, type Host } from "../../toolkit/streamsy.ts";
import type * as Group from "../../toolkit/streamsy.ts";
import type { Storage } from "../storage.ts";
import type { OwnerRecords } from "../../internal/owner-records.ts";
import type { CommitBoundary } from "../commit-boundary.ts";
import { layer as storageLayer, type MemoryOptions } from "./layer.ts";

/** Options for `layer` and `layerProtocol`. */
export interface Options extends MemoryOptions, Protocol.ProtocolOptions {}

/** Generic reader and writer over memory storage, for protocol tools. */
export const layerProtocol = (options: Options = {}) =>
  Protocol.layer(options).pipe(Layer.provideMerge(storageLayer(options)));

/** Memory host. Streams and their feeds share the exposed storage boundary. */
export function layer<const Keys extends ReadonlyArray<Any>>(
  group: Group.Group<Keys>,
  options?: Options,
): Layer.Layer<Storage | CommitBoundary | OwnerRecords | Host<Group.Group<Keys>>>;
export function layer(group: Group.Group, options: Options = {}) {
  const storage = storageLayer(options);
  const protocol = Protocol.layer(options).pipe(Layer.provideMerge(storage));

  return bind(protocol, group).pipe(Layer.provideMerge(storage));
}
