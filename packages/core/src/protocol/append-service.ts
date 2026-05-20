/**
 * Append-side orchestration for the durable streams protocol.
 *
 * Responsibilities:
 *
 *   1. record lookup, lifecycle gate (not-found / soft-deleted)
 *   2. producer state load
 *   3. producer tuple validation
 *   4. early rejection with unchanged result shapes when not accepted
 *   5. lifecycle / content-type / sequence checks
 *   6. body framing and stream mutation (appendMessages / closeRecord)
 *   7. accepted producer-state persistence after successful mutation
 *   8. AppendResult shaping
 *
 * Used by `StreamProtocol.append`. The caller owns stream-level serialization:
 * `StreamProtocol.append` acquires `stream:<streamId>` before calling
 * `execute`, so producer-state read, validation, stream mutation, and
 * accepted-state write remain atomic per stream.
 *
 * The low-level mutation helpers (`appendMessages`, `closeRecord`) are
 * injected as callbacks because they are also used by create/fork flows in
 * `StreamProtocol`.
 */

import type { AppendOptions, AppendResult } from "../types/protocol.ts";
import type { StreamId, StreamRecord, StreamStoreAdapter } from "../types/storage.ts";
import { contentTypeMatches } from "./helpers/content-type-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";
import {
  ProducerIdempotencyService,
  rejectionToAppendResult,
  type ProducerValidation,
} from "./helpers/producer-idempotency-service.ts";

export interface AppendMutators {
  appendMessages(
    streamId: StreamId,
    record: StreamRecord,
    data: Uint8Array[],
    seq?: string,
  ): Promise<string>;
  closeRecord(
    streamId: StreamId,
    record: StreamRecord,
    data: Uint8Array[],
    seq?: string,
  ): Promise<string>;
}

export function appendedResult(
  nextOffset: string,
  validation: ProducerValidation | undefined,
  closed?: boolean,
): AppendResult {
  if (validation?.kind === "accepted")
    return {
      status: "appended",
      nextOffset,
      producerEpoch: validation.proposedState.epoch,
      producerSeq: validation.proposedState.lastSeq,
      closed,
    };
  return { status: "appended", nextOffset, closed };
}

export class AppendService {
  constructor(
    private store: StreamStoreAdapter,
    private producerIdempotency: ProducerIdempotencyService,
    private mutators: AppendMutators,
  ) {}

  async execute(streamId: StreamId, options: AppendOptions): Promise<AppendResult> {
    const record = await this.store.get(streamId);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };

    const isClosed = record.lifecycle.closed === true;
    const wantClose = options.close === true;
    const hasBody = options.data.byteLength > 0;

    let producerValidation: ProducerValidation | undefined;
    if (options.producer) {
      const state = await this.producerIdempotency.load(streamId, options.producer.producerId);
      producerValidation = this.producerIdempotency.validate(
        state,
        options.producer.producerEpoch,
        options.producer.producerSeq,
      );
      if (producerValidation.kind !== "accepted")
        return rejectionToAppendResult(producerValidation, record.currentOffset, isClosed);
    }

    if (wantClose && !hasBody) {
      if (isClosed)
        return { status: "appended", nextOffset: record.currentOffset, closed: true };
      const nextOffset = await this.mutators.closeRecord(streamId, record, [], options.seq);
      await this.producerIdempotency.persistIfAccepted(
        streamId,
        options.producer,
        producerValidation,
      );
      return appendedResult(nextOffset, producerValidation, true);
    }

    if (isClosed)
      return {
        status: "conflict",
        conflictReason: "closed",
        closed: true,
        nextOffset: record.currentOffset,
      };
    if (!contentTypeMatches(record.config.contentType, options.contentType))
      return { status: "conflict", conflictReason: "content-type" };
    if (options.seq && record.lifecycle.lastSeq && options.seq <= record.lifecycle.lastSeq)
      return { status: "conflict", conflictReason: "sequence" };

    const processed = frameMessages(options.data, record.config.contentType);
    const nextOffset = wantClose
      ? await this.mutators.closeRecord(streamId, record, processed, options.seq)
      : await this.mutators.appendMessages(streamId, record, processed, options.seq);
    await this.producerIdempotency.persistIfAccepted(
      streamId,
      options.producer,
      producerValidation,
    );
    return appendedResult(nextOffset, producerValidation, wantClose);
  }
}
