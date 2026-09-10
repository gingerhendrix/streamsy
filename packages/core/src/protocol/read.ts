import { Clock, Effect, Option, Random, Stream } from "effect";
import type { StorageFault } from "../fault.ts";
import type { StreamId, StreamRecord } from "../schema/index.ts";
import { Offset } from "../schema/index.ts";
import { ZERO_OFFSET, isValid } from "../offset/index.ts";
import type { Storage } from "../storage/storage.ts";
import { generateCursor } from "../policy/cursor-generator.ts";
import { expireIfNeeded, touch } from "./expiry.ts";
import type { HeadOutcome, ReadOutcome, ReadNextOutcome } from "./outcomes.ts";
import type { ReadOptions, ReadNextOptions } from "./options.ts";

export const head = Effect.fn("Protocol.head")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
): Effect.fn.Return<HeadOutcome, StorageFault> {
  const found = yield* expireIfNeeded(storage, id);
  if (Option.isNone(found)) return { status: "not-found" };
  const record = found.value;
  if (record.lifecycle.softDeleted) return { status: "gone" };
  return {
    status: "ok",
    contentType: record.config.contentType,
    nextOffset: record.currentOffset,
    ttlSeconds: record.config.ttlSeconds,
    expiresAt: record.config.expiresAt,
    closed: record.lifecycle.closed,
  };
});

const readRecord = Effect.fn("Protocol.readRecord")(function* (
  storage: typeof Storage.Service,
  record: StreamRecord,
  options: ReadOptions,
) {
  const from = options.offset === "now" ? record.currentOffset : options.offset;
  const normalized = !from || from === "-1" ? undefined : from;
  const canonical = normalized !== undefined && isValid(normalized);
  const after = canonical ? Offset.make(normalized) : undefined;
  yield* touch(storage, record);
  // Storage windows use canonical tokens. Preserve the old protocol's lexical
  // handling of other strings without branding an invalid storage offset.
  const raw = yield* storage.messages(record.id, {
    after,
    limit: normalized === undefined || canonical ? options.limit : undefined,
  });
  const messages =
    normalized !== undefined && !canonical
      ? raw
          .filter((message) => message.offset > normalized)
          .slice(0, options.limit === undefined ? undefined : Math.max(0, options.limit))
      : [...raw];
  const nextOffset = messages.at(-1)?.offset ?? record.currentOffset;
  const upToDate = nextOffset === record.currentOffset;
  return {
    status: "ok" as const,
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
): Effect.fn.Return<ReadOutcome, StorageFault> {
  const found = yield* expireIfNeeded(storage, id);
  if (Option.isNone(found)) return { status: "not-found" };
  if (found.value.lifecycle.softDeleted) return { status: "gone" };
  return yield* readRecord(storage, found.value, options);
});

const missing = (status: "not-found" | "gone"): ReadNextOutcome => ({
  status,
  messages: [],
  nextOffset: "",
  upToDate: false,
  cursor: "",
});
function liveResult(
  result: Extract<ReadOutcome, { status: "ok" }>,
  record: StreamRecord,
  from: string,
) {
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
): Effect.fn.Return<ReadNextOutcome, StorageFault> {
  const found = yield* expireIfNeeded(storage, id);
  if (Option.isNone(found)) return missing("not-found");
  const record = found.value;
  if (record.lifecycle.softDeleted) return missing("gone");
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
      status: initial.messages.length > 0 ? "ok" : "timeout",
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
  if (Option.isNone(latest)) return missing("not-found");
  if (latest.value.lifecycle.softDeleted) return missing("gone");
  const result = yield* readRecord(storage, latest.value, { offset: from });
  return {
    ...liveResult(result, latest.value, from),
    status: result.messages.length > 0 || Option.isSome(waited) ? "ok" : "timeout",
    cursor: yield* cursor,
  };
});
