import { Clock, Effect, Predicate, Option } from "effect";
import type { Storage } from "../storage/storage.ts";
import { ProducerId, type StreamId, type StreamLifecycle } from "../schema/index.ts";
import { next } from "../offset/index.ts";
import { contentTypeMatches } from "../policy/content-type-matcher.ts";
import { frameMessages } from "../policy/message-framer.ts";
import {
  rejectionToAppendError,
  validateProducer,
} from "../policy/producer-idempotency-service.ts";
import { expireIfNeeded } from "./expiry.ts";
import type { AppendOptions } from "./options.ts";
import {
  StreamNotFound,
  StreamGone,
  StreamClosed,
  OffsetMismatch,
  AppendConflict,
  StreamBusy,
  InvalidAppendRequest,
  type AppendError,
} from "./errors.ts";
import type { AppendResult } from "./results.ts";

export const append = Effect.fn("Protocol.append")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
  options: AppendOptions,
): Effect.fn.Return<AppendResult, AppendError | import("../fault.ts").StorageFault> {
  // These are semantic replans, not retries of an opaque write or its fault.
  for (let attempt = 0; attempt < 8; attempt++) {
    const found = yield* expireIfNeeded(storage, id);
    if (Option.isNone(found)) return yield* new StreamNotFound({ id });
    const record = found.value;
    if (record.lifecycle.softDeleted) return yield* new StreamGone({ id });
    const producer = options.producer;
    const saved = producer
      ? yield* storage.producer(id, ProducerId.make(producer.producerId))
      : Option.none();
    const validation = producer
      ? validateProducer(Option.getOrUndefined(saved), producer.producerEpoch, producer.producerSeq)
      : undefined;
    // A retried acknowledged tuple wins even over a stale or malformed expected offset.
    if (validation && Predicate.isTagged(validation, "Duplicate"))
      return {
        _tag: "Duplicate",
        offset: record.currentOffset,
        producerEpoch: validation.epoch,
        producerSeq: validation.lastSeq,
        closed: record.lifecycle.closed,
      };
    if (validation && !Predicate.isTagged(validation, "Accepted"))
      return yield* rejectionToAppendError(validation, id);
    const wantClose = options.close === true;
    const closeOnly = wantClose && options.data.byteLength === 0;
    // One append rule for every transport: a write carries at least one message, or
    // it closes the stream with no body. The HTTP edge answers these shapes with 400.
    if (options.data.byteLength === 0 && !closeOnly)
      return yield* new InvalidAppendRequest({ id, message: "Empty append" });
    if (closeOnly && record.lifecycle.closed)
      return { _tag: "Appended", offset: record.currentOffset, closed: true };
    if (record.lifecycle.closed)
      return yield* new StreamClosed({ id, offset: record.currentOffset });
    if (!closeOnly && !contentTypeMatches(record.config.contentType, options.contentType))
      return yield* new AppendConflict({ id, message: "Content-Type mismatch" });
    if (
      !closeOnly &&
      options.seq &&
      record.lifecycle.lastSeq &&
      options.seq <= record.lifecycle.lastSeq
    )
      return yield* new AppendConflict({ id, message: "Sequence conflict" });
    if (options.expectedOffset !== undefined && options.expectedOffset !== record.currentOffset)
      return yield* new OffsetMismatch({
        id,
        expected: options.expectedOffset,
        actual: record.currentOffset,
      });
    const now = yield* Clock.currentTimeMillis;
    let offset = record.currentOffset;
    const messages = (closeOnly ? [] : frameMessages(options.data, record.config.contentType)).map(
      (data) => ({ data, offset: (offset = next(offset)), timestamp: now }),
    );
    // A JSON body can carry no message without being empty: `[]` is not an append.
    if (messages.length === 0 && !closeOnly)
      return yield* new InvalidAppendRequest({ id, message: "Empty append" });
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
    const applied = yield* storage.mutate({ operations: [withProducer] }).pipe(
      Effect.uninterruptible,
      Effect.as(true),
      Effect.catchTag("MutationRejected", () => Effect.succeed(false)),
    );
    if (applied)
      return {
        _tag: "Appended",
        offset,
        closed: wantClose,
        producerEpoch: accepted?.proposedState.epoch,
        producerSeq: accepted?.proposedState.lastSeq,
      };
  }
  return yield* new StreamBusy({ id });
});
