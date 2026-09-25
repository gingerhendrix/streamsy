import type { ProducerOptions } from "../protocol/options.ts";
export type Position = ProducerOptions;
/** Advance only after the caller has acknowledged appended or duplicate. Persist this tuple at the owning edge. */
export const next = (position: Position): Position => ({
  ...position,
  producerSeq: position.producerSeq + 1,
});
