import { Context, Effect, Layer } from "effect";
import { Streams } from "@streamsy/core/protocol";
import { MemoryCommitBoundary, ownerOf, attachOwner } from "@streamsy/core/internal/memory";
import { CheckpointStore, fromStore, type EncodedStore } from "./checkpoint-store.ts";

/** Requires the exact memory owner retained by Streams.layerMemory(). */
export const layer = Layer.effectContext(
  Effect.gen(function* () {
    const boundary = yield* MemoryCommitBoundary;
    const store: EncodedStore = {
      read: boundary.read,
      write: boundary.write,
      remove: boundary.remove,
      withTransaction: boundary.withTransaction,
    };
    const checkpoints = fromStore(store);
    attachOwner(checkpoints.load, ownerOf(boundary));
    return Context.make(CheckpointStore, checkpoints);
  }),
);

/** A single graph retains the real reader, writer and atomic memory owner. */
export const layerMemory = (options: Parameters<typeof Streams.layerMemory>[0] = {}) =>
  layer.pipe(Layer.provideMerge(Streams.layerMemory(options)));
