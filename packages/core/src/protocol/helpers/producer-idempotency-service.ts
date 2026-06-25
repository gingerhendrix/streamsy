/** Pure producer idempotency decisions. */

import type { ProducerState } from "../../types/storage.ts";

export type ProducerValidation =
  | { kind: "accepted"; proposedState: ProducerState }
  | { kind: "duplicate"; lastSeq: number; epoch: number }
  | { kind: "stale-epoch"; currentEpoch: number }
  | { kind: "gap"; expectedSeq: number; receivedSeq: number }
  | { kind: "invalid-epoch-seq" };

export type ProducerRejection = Exclude<ProducerValidation, { kind: "accepted" }>;

export function validateProducer(
  state: ProducerState | undefined,
  epoch: number,
  seq: number,
): ProducerValidation {
  if (!state)
    return seq === 0
      ? { kind: "accepted", proposedState: { epoch, lastSeq: 0 } }
      : { kind: "gap", expectedSeq: 0, receivedSeq: seq };
  if (epoch < state.epoch) return { kind: "stale-epoch", currentEpoch: state.epoch };
  if (epoch > state.epoch)
    return seq === 0
      ? { kind: "accepted", proposedState: { epoch, lastSeq: 0 } }
      : { kind: "invalid-epoch-seq" };
  if (seq <= state.lastSeq)
    return { kind: "duplicate", lastSeq: state.lastSeq, epoch: state.epoch };
  if (seq === state.lastSeq + 1)
    return { kind: "accepted", proposedState: { epoch, lastSeq: seq } };
  return { kind: "gap", expectedSeq: state.lastSeq + 1, receivedSeq: seq };
}

export function rejectionToAppendResult(
  rejection: ProducerRejection,
  currentOffset: string,
  isClosed: boolean,
) {
  switch (rejection.kind) {
    case "duplicate":
      return {
        status: "duplicate" as const,
        offset: currentOffset,
        producerEpoch: rejection.epoch,
        producerSeq: rejection.lastSeq,
        closed: isClosed,
      };
    case "stale-epoch":
      return { status: "stale-epoch" as const, currentEpoch: rejection.currentEpoch };
    case "gap":
      return {
        status: "producer-gap" as const,
        expectedSeq: rejection.expectedSeq,
        receivedSeq: rejection.receivedSeq,
      };
    case "invalid-epoch-seq":
      return { status: "invalid-epoch-seq" as const };
  }
}
