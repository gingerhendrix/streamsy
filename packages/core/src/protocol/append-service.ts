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

/**
 * Mutators return the stream's post-mutation tail offset: the offset of the
 * last message they wrote (or the unchanged tail for a body-less close). That
 * value is both the exact appended offset and, because reads are
 * after-exclusive, the read cursor.
 */
export function appendedResult(
  offset: string,
  validation: ProducerValidation | undefined,
  closed?: boolean,
): AppendResult {
  if (validation?.kind === "accepted")
    return {
      status: "appended",
      offset,
      producerEpoch: validation.proposedState.epoch,
      producerSeq: validation.proposedState.lastSeq,
      closed,
    };
  return { status: "appended", offset, closed };
}

/**
 * Compare-and-swap precondition for optimistic concurrency: when
 * `expectedOffset` is set, the append proceeds only if the stream's tail
 * still equals it. Atomicity with the mutation is guaranteed because the
 * protocol loads the record and runs this service inside the per-stream
 * mutation lock.
 *
 * Pinned precedence: checked after the closed/content-type/sequence conflicts
 * and before any mutation, so a CAS failure never advances producer state.
 * A close-only append on an already-closed stream stays an idempotent
 * success and skips the check (nothing is written, so no update can be lost).
 */
function expectedOffsetConflict(record: StreamRecord, options: AppendOptions): AppendResult | null {
  if (options.expectedOffset === undefined || options.expectedOffset === record.currentOffset)
    return null;
  return { status: "conflict", conflictReason: "expected-offset", offset: record.currentOffset };
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
      if (isClosed)
        return {
          status: "appended",
          offset: record.currentOffset,
          closed: true,
        };
      const casConflict = expectedOffsetConflict(record, options);
      if (casConflict) return casConflict;
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
        offset: record.currentOffset,
      };
    if (!contentTypeMatches(record.config.contentType, options.contentType))
      return { status: "conflict", conflictReason: "content-type" };
    if (options.seq && record.lifecycle.lastSeq && options.seq <= record.lifecycle.lastSeq)
      return { status: "conflict", conflictReason: "sequence" };
    const casConflict = expectedOffsetConflict(record, options);
    if (casConflict) return casConflict;

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
