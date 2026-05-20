/**
 * Producer idempotency state-transition decisions and persistence.
 *
 * Responsibilities:
 *
 * - first appended `seq` from a producer must be 0;
 * - same-epoch `seq <= lastSeq` is a duplicate (echoes stored epoch/lastSeq);
 * - epoch lower than the stored epoch is `stale-epoch`;
 * - epoch higher than the stored epoch is accepted only with `seq === 0`,
 *   otherwise reported as `invalid-epoch-seq`;
 * - same-epoch next sequence is accepted only when `seq === lastSeq + 1`;
 * - same-epoch gap reports the expected and received seq;
 * - accepted state is persisted through `StreamStoreAdapter.setProducerState`.
 *
 * Used by `AppendService`. Callers must invoke this service inside the
 * stream-level `withLock` critical section to keep producer-state and
 * stream-mutation atomic.
 */

import type { AppendResult, ProducerOptions } from "../../types/protocol.ts";
import type { ProducerState, StreamId, StreamStoreAdapter } from "../../types/storage.ts";

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
): AppendResult {
  switch (rejection.kind) {
    case "duplicate":
      return {
        status: "duplicate",
        nextOffset: currentOffset,
        producerEpoch: rejection.epoch,
        producerSeq: rejection.lastSeq,
        closed: isClosed,
      };
    case "stale-epoch":
      return { status: "stale-epoch", currentEpoch: rejection.currentEpoch };
    case "gap":
      return {
        status: "producer-gap",
        expectedSeq: rejection.expectedSeq,
        receivedSeq: rejection.receivedSeq,
      };
    case "invalid-epoch-seq":
      return { status: "invalid-epoch-seq" };
  }
}

export class ProducerIdempotencyService {
  constructor(private store: StreamStoreAdapter) {}

  load(streamId: StreamId, producerId: string): Promise<ProducerState | undefined> {
    return this.store.getProducerState(streamId, producerId);
  }

  validate(
    state: ProducerState | undefined,
    epoch: number,
    seq: number,
  ): ProducerValidation {
    return validateProducer(state, epoch, seq);
  }

  async persistIfAccepted(
    streamId: StreamId,
    producer: ProducerOptions | undefined,
    validation: ProducerValidation | undefined,
  ): Promise<void> {
    if (!producer || validation?.kind !== "accepted") return;
    await this.store.setProducerState(streamId, producer.producerId, validation.proposedState);
  }
}
