import { Clock, Effect, Predicate, Option } from "effect";
import type { Storage } from "../storage/storage.ts";
import { ProducerId, type StreamId, type StreamLifecycle } from "../schema/index.ts";
import { next } from "../offset/index.ts";
import { contentTypeMatches } from "../policy/content-type-matcher.ts";
import { frameMessages } from "../policy/message-framer.ts";
import {
  rejectionToAppendResult,
  validateProducer,
} from "../policy/producer-idempotency-service.ts";
import { expireIfNeeded } from "./expiry.ts";
import type { AppendOptions } from "./options.ts";
import type { AppendOutcome } from "./outcomes.ts";

export const append = Effect.fn("Protocol.append")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
  options: AppendOptions,
): Effect.fn.Return<AppendOutcome, import("../fault.ts").StorageFault> {
  // These are semantic replans, not retries of an opaque write or its fault.
  for (let attempt = 0; attempt < 8; attempt++) {
    const found = yield* expireIfNeeded(storage, id);
    if (Option.isNone(found)) return { status: "not-found" };
    const record = found.value;
    if (record.lifecycle.softDeleted) return { status: "gone" };
    const producer = options.producer;
    const saved = producer
      ? yield* storage.producer(id, ProducerId.make(producer.producerId))
      : Option.none();
    const validation = producer
      ? validateProducer(Option.getOrUndefined(saved), producer.producerEpoch, producer.producerSeq)
      : undefined;
    // A retried acknowledged tuple wins even over a stale or malformed expected offset.
    if (validation && !Predicate.isTagged(validation, "Accepted"))
      return rejectionToAppendResult(validation, record.currentOffset, record.lifecycle.closed);
    const wantClose = options.close === true;
    const closeOnly = wantClose && options.data.byteLength === 0;
    if (closeOnly && record.lifecycle.closed)
      return { status: "appended", offset: record.currentOffset, closed: true };
    if (record.lifecycle.closed)
      return {
        status: "conflict",
        conflictReason: "closed",
        offset: record.currentOffset,
        closed: true,
      };
    if (!closeOnly && !contentTypeMatches(record.config.contentType, options.contentType))
      return { status: "conflict", conflictReason: "content-type" };
    if (
      !closeOnly &&
      options.seq &&
      record.lifecycle.lastSeq &&
      options.seq <= record.lifecycle.lastSeq
    )
      return { status: "conflict", conflictReason: "sequence" };
    if (options.expectedOffset !== undefined && options.expectedOffset !== record.currentOffset)
      return {
        status: "conflict",
        conflictReason: "expected-offset",
        offset: record.currentOffset,
      };
    const now = yield* Clock.currentTimeMillis;
    let offset = record.currentOffset;
    const messages = (closeOnly ? [] : frameMessages(options.data, record.config.contentType)).map(
      (data) => ({ data, offset: (offset = next(offset)), timestamp: now }),
    );
    const lifecycle: { -readonly [K in keyof StreamLifecycle]?: StreamLifecycle[K] } = {};
    if (options.seq) lifecycle.lastSeq = options.seq;
    if (wantClose) {
      lifecycle.closed = true;
      lifecycle.closedAt = now;
    }
    if (record.config.ttlSeconds !== undefined)
      lifecycle.expiresAtMs = now + record.config.ttlSeconds * 1000;
    const accepted =
      validation && Predicate.isTagged(validation, "Accepted") ? validation : undefined;
    const operation = {
      _tag: "Append" as const,
      streamId: id,
      messages,
      patch: { currentOffset: offset, lifecycle },
      expectedOffset: record.currentOffset,
      expectedClosed: false,
    };
    const withProducer =
      producer && accepted
        ? {
            ...operation,
            producer: {
              producerId: ProducerId.make(producer.producerId),
              expected: saved,
              next: accepted.proposedState,
            },
          }
        : operation;
    const outcome = yield* storage
      .mutate({ operations: [withProducer] })
      .pipe(Effect.uninterruptible);
    if (Predicate.isTagged(outcome, "Applied"))
      return {
        status: "appended",
        offset,
        closed: wantClose,
        producerEpoch: accepted?.proposedState.epoch,
        producerSeq: accepted?.proposedState.lastSeq,
      };
  }
  return { status: "busy" };
});
