import { Layer } from "effect";
import * as Protocol from "../protocol/layer.ts";
import * as Memory from "../storage/memory/layer.ts";
export const layerMemory = (options: Memory.MemoryOptions & Protocol.ProtocolOptions = {}) =>
  Protocol.layer(options).pipe(Layer.provideMerge(Memory.layer(options)));
