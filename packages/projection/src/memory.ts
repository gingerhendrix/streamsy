import { Context, Effect, Layer } from "effect";
import { Streams } from "@streamsy/core";
import { MemoryCommitBoundary } from "@streamsy/core/internal/memory";
import { Checkpoints, fromStore, type EncodedStore } from "./checkpoint.ts";
import { State, stateFromStore } from "./state.ts";

/** Requires the exact memory owner retained by Streams.layerMemory(). */
export const layer = Layer.effectContext(
  Effect.gen(function* () {
    const boundary = yield* MemoryCommitBoundary;
    const stateKey = (key: string) => `state:${key}`;
    const store: EncodedStore = {
      read: boundary.read,
      write: boundary.write,
      remove: boundary.remove,
      readState: (key) => boundary.read(stateKey(key)),
      writeState: (key, value) => boundary.write(stateKey(key), value),
      removeState: (key) => boundary.remove(stateKey(key)),
      withTransaction: boundary.withTransaction,
    };
    return Context.make(Checkpoints, fromStore(store)).pipe(
      Context.add(State, stateFromStore(store)),
    );
  }),
);

/** A single graph retains the real reader, writer and fused memory owner. */
export const layerMemory = (options: Parameters<typeof Streams.layerMemory>[0] = {}) =>
  layer.pipe(Layer.provideMerge(Streams.layerMemory(options)));
