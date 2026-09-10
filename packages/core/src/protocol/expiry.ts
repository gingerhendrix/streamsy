import { Clock, Effect, Option } from "effect";
import { Storage } from "../storage/storage.ts";
import { isExpired } from "../policy/expiry-policy.ts";
import type { StreamId, StreamRecord } from "../schema/index.ts";

export const expireIfNeeded = Effect.fn("Protocol.expireIfNeeded")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
) {
  const record = yield* storage.record(id);
  const now = yield* Clock.currentTimeMillis;
  if (
    Option.isSome(record) &&
    !record.value.lifecycle.softDeleted &&
    isExpired(record.value, now)
  ) {
    yield* storage
      .mutate({
        operations: [
          {
            _tag: "Delete",
            streamId: id,
            reason: "expiry",
            expectedExpiresAtMs: record.value.lifecycle.expiresAtMs,
          },
        ],
      })
      .pipe(
        Effect.uninterruptible,
        // A concurrent renewal or deletion wins; reread the record below.
        Effect.catchTag("MutationRejected", () => Effect.void),
      );
    return yield* storage.record(id);
  }
  return record;
});

export const touch = Effect.fn("Protocol.touch")(function* (
  storage: typeof Storage.Service,
  record: StreamRecord,
) {
  if (record.config.ttlSeconds === undefined) return;
  const now = yield* Clock.currentTimeMillis;
  yield* storage
    .mutate({
      operations: [
        {
          _tag: "Append",
          streamId: record.id,
          messages: [],
          expectedOffset: record.currentOffset,
          patch: { lifecycle: { expiresAtMs: now + record.config.ttlSeconds * 1000 } },
        },
      ],
    })
    .pipe(
      Effect.uninterruptible,
      // A concurrent append or deletion wins over this best-effort TTL touch.
      Effect.catchTag("MutationRejected", () => Effect.void),
    );
});

/** One host-triggered sweep; the host owns scheduling and cancellation. */
export const expireDue = Effect.fn("Protocol.expireDue")(function* () {
  const storage = yield* Storage;
  const now = yield* Clock.currentTimeMillis;
  while (true) {
    const deadline = yield* storage.nextExpiry;
    if (Option.isNone(deadline) || deadline.value.at > now) return;
    yield* storage
      .mutate({
        operations: [
          {
            _tag: "Delete",
            streamId: deadline.value.streamId,
            reason: "expiry",
            expectedExpiresAtMs: deadline.value.at,
          },
        ],
      })
      .pipe(
        Effect.uninterruptible,
        // A stale deadline loses; the next indexed observation must advance.
        Effect.catchTag("MutationRejected", () => Effect.void),
      );
  }
});
