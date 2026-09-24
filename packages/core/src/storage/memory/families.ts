import { Layer } from "effect";
import * as Protocol from "../../protocol/layer.ts";
import { provides, type AnyFamily, type Identifiers } from "../../toolkit/family.ts";
import type * as Group from "../../toolkit/group.ts";
import type { Changes } from "../../toolkit/changes.ts";
import type { Storage } from "../storage.ts";
import type { MemoryCommitBoundary } from "./boundary.ts";
import { layer as storageLayer, type MemoryOptions } from "./layer.ts";
export type { MemoryOptions } from "./layer.ts";

/** Memory host. Families and their feeds share the exposed storage boundary. */
export function layer<const Keys extends ReadonlyArray<AnyFamily>>(
  options: MemoryOptions & Protocol.ProtocolOptions,
  ...input: Group.NonEmptyInput<Keys>
): Layer.Layer<Storage | MemoryCommitBoundary | Identifiers<Keys> | Changes<Identifiers<Keys>>>;
export function layer(options?: MemoryOptions): ReturnType<typeof storageLayer>;
export function layer(
  options: MemoryOptions & Protocol.ProtocolOptions = {},
  ...input: Group.Input<ReadonlyArray<AnyFamily>>
) {
  const storage = storageLayer(options);
  if (input.length === 0) return storage;
  const protocol = Protocol.layer(options).pipe(Layer.provideMerge(storage));
  return provides(protocol, ...input).pipe(Layer.provideMerge(storage));
}
