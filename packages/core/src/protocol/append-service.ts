/** Append-side plan building for one storage-bound stream snapshot. */

import type { MutationPlan } from "../types/factory.ts";
import type { AppendOptions, AppendResult } from "../types/protocol.ts";
import type { Clock, ProducerState, StreamRecord } from "../types/storage.ts";
import { contentTypeMatches } from "./helpers/content-type-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";
import { allocate as allocateOffsets } from "./helpers/offset-generator.ts";
import {
  rejectionToAppendResult,
  validateProducer,
  type ProducerValidation,
} from "./helpers/producer-idempotency-service.ts";

export type AppendDecision =
  | { kind: "terminal"; result: AppendResult }
  | { kind: "commit"; plan: MutationPlan; toResult: (record: StreamRecord) => AppendResult };

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

function expectedOffsetConflict(record: StreamRecord, options: AppendOptions): AppendResult | null {
  if (options.expectedOffset === undefined || options.expectedOffset === record.currentOffset)
    return null;
  return { status: "conflict", conflictReason: "expected-offset", offset: record.currentOffset };
}

export interface AppendServiceDeps {
  clock: Clock;
}

export class AppendService {
  constructor(private deps: AppendServiceDeps) {}

  plan(
    record: StreamRecord | null,
    options: AppendOptions,
    producerState: ProducerState | undefined,
  ): AppendDecision {
    if (!record) return { kind: "terminal", result: { status: "not-found" } };
    if (record.lifecycle.softDeleted) return { kind: "terminal", result: { status: "gone" } };

    const isClosed = record.lifecycle.closed === true;
    const wantClose = options.close === true;
    const hasBody = options.data.byteLength > 0;

    let producerValidation: ProducerValidation | undefined;
    if (options.producer) {
      producerValidation = validateProducer(
        producerState,
        options.producer.producerEpoch,
        options.producer.producerSeq,
      );
      if (producerValidation.kind !== "accepted") {
        return {
          kind: "terminal",
          result: rejectionToAppendResult(producerValidation, record.currentOffset, isClosed),
        };
      }
    }

    if (wantClose && !hasBody) {
      if (isClosed)
        return {
          kind: "terminal",
          result: {
            status: "appended",
            offset: record.currentOffset,
            closed: true,
          },
        };
      const casConflict = expectedOffsetConflict(record, options);
      if (casConflict) return { kind: "terminal", result: casConflict };
      return this.commitDecision(record, [], options, producerState, producerValidation, true);
    }

    if (isClosed)
      return {
        kind: "terminal",
        result: {
          status: "conflict",
          conflictReason: "closed",
          closed: true,
          offset: record.currentOffset,
        },
      };
    if (!contentTypeMatches(record.config.contentType, options.contentType))
      return { kind: "terminal", result: { status: "conflict", conflictReason: "content-type" } };
    if (options.seq && record.lifecycle.lastSeq && options.seq <= record.lifecycle.lastSeq)
      return { kind: "terminal", result: { status: "conflict", conflictReason: "sequence" } };
    const casConflict = expectedOffsetConflict(record, options);
    if (casConflict) return { kind: "terminal", result: casConflict };

    const processed = frameMessages(options.data, record.config.contentType);
    return this.commitDecision(
      record,
      processed,
      options,
      producerState,
      producerValidation,
      wantClose,
    );
  }

  private commitDecision(
    record: StreamRecord,
    data: Uint8Array[],
    options: AppendOptions,
    producerState: ProducerState | undefined,
    producerValidation: ProducerValidation | undefined,
    wantClose: boolean,
  ): AppendDecision {
    const allocation = allocateOffsets(record.counter, data.length);
    const now = this.deps.clock.now();
    const messages = data.map((bytes, i) => ({
      data: bytes,
      offset: allocation.offsets[i]!,
      timestamp: now,
    }));
    const expiresAtMs =
      record.config.ttlSeconds === undefined ? undefined : now + record.config.ttlSeconds * 1000;
    const lifecycle = {
      ...(options.seq ? { lastSeq: options.seq } : {}),
      ...(wantClose ? { closed: true, closedAt: now } : {}),
      ...(expiresAtMs !== undefined ? { expiresAtMs } : {}),
    };
    const plan: MutationPlan = {
      preconditions: {
        expectedOffset: options.expectedOffset ?? record.currentOffset,
        expectedClosed: false,
        ...(options.producer && producerValidation?.kind === "accepted"
          ? {
              producer: {
                producerId: options.producer.producerId,
                expected: producerState,
                next: producerValidation.proposedState,
              },
            }
          : {}),
      },
      appendMessages: messages,
      recordPatch: {
        currentOffset: allocation.nextOffset,
        counter: allocation.endCounter,
        lifecycle,
      },
      afterCommit: {
        notify: wantClose ? "closed" : "message",
        ...(expiresAtMs !== undefined ? { scheduleExpiryAt: expiresAtMs } : {}),
      },
    };

    return {
      kind: "commit",
      plan,
      toResult: () => appendedResult(allocation.nextOffset, producerValidation, wantClose),
    };
  }
}
