import { Effect } from "effect";
import type { StreamRef } from "./ref.ts";
import type { AppendOptions, ProducerOptions } from "../protocol/options.ts";
import * as Streams from "./streams.ts";
export type Position = ProducerOptions;
/** Advance only after the caller has acknowledged appended or duplicate. Persist this tuple at the owning edge. */
export const next = (position: Position): Position => ({
  ...position,
  producerSeq: position.producerSeq + 1,
});
export const append = Effect.fn("Producer.append")(function* <A, RD, RE>(
  ref: StreamRef<A, RD, RE>,
  items: ReadonlyArray<A>,
  position: Position,
  options: Omit<AppendOptions, "data" | "contentType" | "producer"> = {},
) {
  return yield* Streams.append(ref, items, {
    ...options,
    producer: position,
  });
});
