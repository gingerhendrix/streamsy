/** Pure producer idempotency decisions. */

import { StaleEpoch, ProducerGap, InvalidEpochSeq } from "../protocol/errors.ts";
import type { StreamId } from "../schema/index.ts";
import type { ProducerState } from "../schema/index.ts";

export type ProducerValidation =
  | { _tag: "Accepted"; proposedState: ProducerState }
  | { _tag: "Duplicate"; lastSeq: number; epoch: number }
  | { _tag: "StaleEpoch"; currentEpoch: number }
  | { _tag: "Gap"; expectedSeq: number; receivedSeq: number }
  | { _tag: "InvalidEpochSeq" };

export type ProducerRejection = Exclude<ProducerValidation, { _tag: "Accepted" }>;

export function validateProducer(
  state: ProducerState | undefined,
  epoch: number,
  seq: number,
): ProducerValidation {
  if (!state)
    return seq === 0
      ? { _tag: "Accepted", proposedState: { epoch, lastSeq: 0 } }
      : { _tag: "Gap", expectedSeq: 0, receivedSeq: seq };
  if (epoch < state.epoch) return { _tag: "StaleEpoch", currentEpoch: state.epoch };
  if (epoch > state.epoch)
    return seq === 0
      ? { _tag: "Accepted", proposedState: { epoch, lastSeq: 0 } }
      : { _tag: "InvalidEpochSeq" };
  if (seq <= state.lastSeq)
    return { _tag: "Duplicate", lastSeq: state.lastSeq, epoch: state.epoch };
  if (seq === state.lastSeq + 1)
    return { _tag: "Accepted", proposedState: { epoch, lastSeq: seq } };
  return { _tag: "Gap", expectedSeq: state.lastSeq + 1, receivedSeq: seq };
}

export function rejectionToAppendError(
  rejection: Exclude<ProducerRejection, { _tag: "Duplicate" }>,
  id: StreamId,
) {
  switch (rejection._tag) {
    case "StaleEpoch":
      return new StaleEpoch({ id, currentEpoch: rejection.currentEpoch });
    case "Gap":
      return new ProducerGap({
        id,
        expectedSeq: rejection.expectedSeq,
        receivedSeq: rejection.receivedSeq,
      });
    case "InvalidEpochSeq":
      return new InvalidEpochSeq({ id });
  }
}
