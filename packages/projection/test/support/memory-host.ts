import { Layer } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore } from "@streamsy/projection";

/** One memory owner for the protocol streams and the checkpoints. */
export const memoryHost = (options: Parameters<typeof MemoryStorage.layerProtocol>[0] = {}) =>
  CheckpointStore.layer.pipe(Layer.provideMerge(MemoryStorage.layerProtocol(options)));
