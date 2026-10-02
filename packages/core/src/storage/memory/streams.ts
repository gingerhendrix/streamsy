import { Layer } from "effect";
import * as Protocol from "../../protocol/layer.ts";
import { bind, type Any, type Identifiers } from "../../toolkit/streamsy.ts";
import type * as Group from "../../toolkit/streamsy.ts";
import type { ChangeFeed } from "../../toolkit/changes.ts";
import type { Storage } from "../storage.ts";
import type { OwnerRecords } from "../../internal/owner-records.ts";
import type { CommitBoundary } from "../commit-boundary.ts";
import { layer as storageLayer, type MemoryOptions } from "./layer.ts";
export type { MemoryOptions } from "./layer.ts";

/** Memory host. Streams and their feeds share the exposed storage boundary. */
export function layer<const Keys extends ReadonlyArray<Any>>(
  group: Group.Group<Keys>,
  options?: MemoryOptions & Protocol.ProtocolOptions,
): Layer.Layer<
  Storage | CommitBoundary | OwnerRecords | Identifiers<Keys> | ChangeFeed<Identifiers<Keys>>
>;
export function layer(group: Group.Group, options: MemoryOptions & Protocol.ProtocolOptions = {}) {
  const storage = storageLayer(options);
  const protocol = Protocol.layer(options).pipe(Layer.provideMerge(storage));
  return bind(protocol, group).pipe(Layer.provideMerge(storage));
}
