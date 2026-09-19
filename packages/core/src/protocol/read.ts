import { Clock, Effect, Option, Random, Stream } from "effect";
import type { StorageFault } from "../fault.ts";
import type { StreamId, StreamRecord } from "../schema/index.ts";
import { Offset } from "../schema/index.ts";
import { ZERO_OFFSET, isValid } from "../offset/index.ts";
import type { Storage } from "../storage/storage.ts";
import { generateCursor } from "../policy/cursor-generator.ts";
import { expireIfNeeded, touch } from "./expiry.ts";
import {
  InvalidReadRequest,
  StreamNotFound,
  StreamGone,
  type HeadError,
  type ReadError,
  type ReadNextError,
} from "./errors.ts";
import type { HeadResult, ReadResult, ReadNextResult } from "./results.ts";
import type { ReadOptions, ReadNextOptions } from "./options.ts";

export const head = Effect.fn("Protocol.head")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
): Effect.fn.Return<HeadResult, HeadError | StorageFault> {
  const found = yield* expireIfNeeded(storage, id);
  if (Option.isNone(found)) return yield* new StreamNotFound({ id });
  const record = found.value;
  if (record.lifecycle.softDeleted) return yield* new StreamGone({ id });
  return {
    contentType: record.config.contentType,
    nextOffset: record.currentOffset,
    ttlSeconds: record.config.ttlSeconds,
    expiresAt: record.config.expiresAt,
    closed: record.lifecycle.closed,
  };
});

const validateOffset = (id: StreamId, offset: string | undefined) =>
  offset === undefined || offset === "-1" || offset === "now" || isValid(offset)
    ? Effect.void
    : Effect.fail(new InvalidReadRequest({ id, message: "Invalid offset format" }));

const readRecord = Effect.fn("Protocol.readRecord")(function* (
  storage: typeof Storage.Service,
  record: StreamRecord,
  options: ReadOptions,
  limit?: number,
) {
  const from = options.offset === "now" ? record.currentOffset : options.offset;
  const normalized = !from || from === "-1" ? undefined : from;
  const after = normalized === undefined ? undefined : Offset.make(normalized);
  yield* touch(storage, record);
  const messages = yield* storage.messages(record.id, { after, limit });
  const nextOffset = messages.at(-1)?.offset ?? record.currentOffset;
  const upToDate = nextOffset === record.currentOffset;
  return {
    contentType: record.config.contentType,
    messages: messages.map((message) => ({ data: message.data })),
    nextOffset,
    upToDate,
    closed: record.lifecycle.closed && upToDate,
  };
});
export const read = Effect.fn("Protocol.read")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
  options: ReadOptions = {},
  readLimit = 1000,
): Effect.fn.Return<ReadResult, ReadError | StorageFault> {
  yield* validateOffset(id, options.offset);
  const found = yield* expireIfNeeded(storage, id);
  if (Option.isNone(found)) return yield* new StreamNotFound({ id });
  if (found.value.lifecycle.softDeleted) return yield* new StreamGone({ id });
  return yield* readRecord(storage, found.value, options, readLimit);
});

function liveResult(result: ReadResult, record: StreamRecord, from: string) {
  // A storage observer can report the tail before a subsequent message read
  // exposes it. Keep the cursor behind invisible data so a later read repairs it.
  const nextOffset =
    result.messages.length === 0 && from < record.currentOffset ? from : result.nextOffset;
  return {
    ...result,
    nextOffset,
    upToDate: true,
    closed: record.lifecycle.closed && nextOffset === record.currentOffset,
  };
}
export const readNext = Effect.fn("Protocol.readNext")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
  options: ReadNextOptions,
  timeoutMs: number,
): Effect.fn.Return<ReadNextResult, ReadNextError | StorageFault> {
  yield* validateOffset(id, options.offset);
  const found = yield* expireIfNeeded(storage, id);
  if (Option.isNone(found)) return yield* new StreamNotFound({ id });
  const record = found.value;
  if (record.lifecycle.softDeleted) return yield* new StreamGone({ id });
  const from =
    options.offset === "now"
      ? record.currentOffset
      : isValid(options.offset)
        ? options.offset
        : ZERO_OFFSET;
  const initial = yield* readRecord(storage, record, { offset: from });
  const cursor = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const random = yield* Random.next;
    // Preserve the ported cursor's lenient parseInt semantics, including malformed input.
    return generateCursor({ now: () => now }, options.cursor, () => random);
  });
  if (initial.messages.length > 0 || record.lifecycle.closed)
    return {
      ...initial,
      upToDate: true,
      timedOut: initial.messages.length === 0,
      cursor: yield* cursor,
    };
  const waited = yield* storage.changes(id).pipe(
    Stream.filter(
      (value) =>
        !value.present ||
        value.currentOffset !== record.currentOffset ||
        value.closed !== record.lifecycle.closed ||
        value.softDeleted !== record.lifecycle.softDeleted,
    ),
    Stream.take(1),
    Stream.runCollect,
    Effect.timeoutOption(timeoutMs),
  );
  // The timeout and wake paths both consult authoritative state after subscription teardown.
  const latest = yield* expireIfNeeded(storage, id);
  if (Option.isNone(latest)) return yield* new StreamNotFound({ id });
  if (latest.value.lifecycle.softDeleted) return yield* new StreamGone({ id });
  const result = yield* readRecord(storage, latest.value, { offset: from });
  return {
    ...liveResult(result, latest.value, from),
    timedOut: result.messages.length === 0 && Option.isNone(waited),
    cursor: yield* cursor,
  };
});
