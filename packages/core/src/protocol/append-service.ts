/** Append-side orchestration for one storage-bound stream. */

import type { AppendOptions, AppendResult } from "../types/protocol.ts";
import { isNotSupported } from "../types/factory.ts";
import type { StreamRecord } from "../types/storage.ts";
import { contentTypeMatches } from "./helpers/content-type-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";
import {
  ProducerIdempotencyService,
  rejectionToAppendResult,
  type ProducerValidation,
} from "./helpers/producer-idempotency-service.ts";

export interface AppendMutators {
  appendMessages(record: StreamRecord, data: Uint8Array[], seq?: string): Promise<string>;
  closeRecord(record: StreamRecord, data: Uint8Array[], seq?: string): Promise<string>;
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

export interface AppendServiceDeps {
  producerIdempotency: ProducerIdempotencyService;
  mutators: AppendMutators;
}

export class AppendService {
  constructor(private deps: AppendServiceDeps) {}

  async execute(record: StreamRecord | null, options: AppendOptions): Promise<AppendResult> {
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };

    const isClosed = record.lifecycle.closed === true;
    const wantClose = options.close === true;
    const hasBody = options.data.byteLength > 0;

    let producerValidation: ProducerValidation | undefined;
    if (options.producer) {
      const state = await this.deps.producerIdempotency.load(options.producer.producerId);
      if (isNotSupported(state)) return state;
      producerValidation = this.deps.producerIdempotency.validate(
        state,
        options.producer.producerEpoch,
        options.producer.producerSeq,
      );
      if (producerValidation.kind !== "accepted")
        return rejectionToAppendResult(producerValidation, record.currentOffset, isClosed);
    }

    if (wantClose && !hasBody) {
      if (isClosed) return { status: "appended", nextOffset: record.currentOffset, closed: true };
      const nextOffset = await this.deps.mutators.closeRecord(record, [], options.seq);
      const persisted = await this.deps.producerIdempotency.persistIfAccepted(
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
      ? await this.deps.mutators.closeRecord(record, processed, options.seq)
      : await this.deps.mutators.appendMessages(record, processed, options.seq);
    const persisted = await this.deps.producerIdempotency.persistIfAccepted(
      options.producer,
      producerValidation,
    );
    if (persisted?.status === "not-supported") return persisted;
    return appendedResult(nextOffset, producerValidation, wantClose);
  }
}
