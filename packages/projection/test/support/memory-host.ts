import { Layer } from "effect";
import { Streams } from "@streamsy/core/protocol";
import { CheckpointStore } from "@streamsy/projection";

/** One memory owner for the protocol streams and the checkpoints. */
export const memoryHost = (options: Parameters<typeof Streams.layerMemory>[0] = {}) =>
  CheckpointStore.layer.pipe(Layer.provideMerge(Streams.layerMemory(options)));
