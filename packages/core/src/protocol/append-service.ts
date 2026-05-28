/** Append-side orchestration for one storage-bound stream. */

import type { AppendOptions, AppendResult } from "../types/protocol.ts";
import { isNotSupported, type Stream } from "../types/factory.ts";
import type { StreamId, StreamRecord } from "../types/storage.ts";
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
    private stream: Stream,
    private producerIdempotency: ProducerIdempotencyService,
    private mutators: AppendMutators,
  ) {}

  async execute(streamId: StreamId, options: AppendOptions): Promise<AppendResult> {
    const record = await this.stream.getRecord();
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };

    const isClosed = record.lifecycle.closed === true;
    const wantClose = options.close === true;
    const hasBody = options.data.byteLength > 0;

    let producerValidation: ProducerValidation | undefined;
    if (options.producer) {
      const state = await this.producerIdempotency.load(options.producer.producerId);
      if (isNotSupported(state)) return state;
      producerValidation = this.producerIdempotency.validate(
        state,
        options.producer.producerEpoch,
        options.producer.producerSeq,
      );
      if (producerValidation.kind !== "accepted")
        return rejectionToAppendResult(producerValidation, record.currentOffset, isClosed);
    }

    if (wantClose && !hasBody) {
      if (isClosed) return { status: "appended", nextOffset: record.currentOffset, closed: true };
      const nextOffset = await this.mutators.closeRecord(streamId, record, [], options.seq);
      const persisted = await this.producerIdempotency.persistIfAccepted(
        options.producer,
        producerValidation,
      );
      if (persisted?.status === "not-supported") return persisted;
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
    const persisted = await this.producerIdempotency.persistIfAccepted(
      options.producer,
      producerValidation,
    );
    if (persisted?.status === "not-supported") return persisted;
    return appendedResult(nextOffset, producerValidation, wantClose);
  }
}
