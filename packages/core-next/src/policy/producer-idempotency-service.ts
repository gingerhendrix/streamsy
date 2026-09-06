/** Pure producer idempotency decisions. */

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

export function rejectionToAppendResult(
  rejection: ProducerRejection,
  currentOffset: string,
  isClosed: boolean,
) {
  switch (rejection._tag) {
    case "Duplicate":
      return {
        status: "duplicate" as const,
        offset: currentOffset,
        producerEpoch: rejection.epoch,
        producerSeq: rejection.lastSeq,
        closed: isClosed,
      };
    case "StaleEpoch":
      return { status: "stale-epoch" as const, currentEpoch: rejection.currentEpoch };
    case "Gap":
      return {
        status: "producer-gap" as const,
        expectedSeq: rejection.expectedSeq,
        receivedSeq: rejection.receivedSeq,
      };
    case "InvalidEpochSeq":
      return { status: "invalid-epoch-seq" as const };
  }
  return exhaustive(rejection);
}

function exhaustive(value: never): never {
  throw new TypeError(`Unexpected producer rejection: ${String(value)}`);
}
