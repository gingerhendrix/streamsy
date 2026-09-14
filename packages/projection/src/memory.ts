import { Effect, Layer } from "effect";
import { Streams } from "@streamsy/core";
import { MemoryCommitBoundary } from "@streamsy/core/storage";
import { Checkpoints, fromStore } from "./checkpoint.ts";

/** Requires the exact memory owner retained by Streams.layerMemory(). */
export const layer = Layer.effect(
  Checkpoints,
  Effect.gen(function* () {
    const boundary = yield* MemoryCommitBoundary;
    return Checkpoints.of(
      fromStore({
        read: boundary.read,
        write: boundary.write,
        withTransaction: boundary.withTransaction,
      }),
    );
  }),
);

/** A single graph retains the real reader, writer and fused memory owner. */
export const layerMemory = (options: Parameters<typeof Streams.layerMemory>[0] = {}) =>
  layer.pipe(Layer.provideMerge(Streams.layerMemory(options)));
