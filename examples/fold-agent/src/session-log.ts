import {
  decodeStoredLogEntry,
  EventLogCorruptEntryError,
  EventLogUnavailableError,
  type LogEntry,
} from "@humanlayer/fold-core";
import { StreamRef, Streams, ZERO_OFFSET } from "@streamsy/core";
import { Effect, Schema, Stream } from "effect";

export const sessionRef = (streamId: string) => {
  if (!/^fold\/sessions\/.+\/events$/.test(streamId))
    throw new TypeError("Expected fold/sessions/<id>/events");
  return StreamRef.json(streamId, { schema: Schema.Json });
};

export const unavailable = (message: string, retryable = false, cause?: unknown) =>
  new EventLogUnavailableError({
    operation: "append",
    message,
    retryable,
    ...(cause === undefined ? {} : { cause }),
  });

export const corrupt = (message: string) =>
  new EventLogCorruptEntryError({ operation: "entries", message });

export const readHistory = Effect.fn("Fold.readHistory")(function* <A>(
  ref: StreamRef.StreamRef<A>,
) {
  const batches = yield* Stream.runCollect(Streams.read(ref));
  return {
    items: batches.flatMap((batch) => batch.items),
    offset: batches.at(-1)?.nextOffset ?? ZERO_OFFSET,
  };
});

export const decodeLog = Effect.fn("Fold.decodeLog")(function* (values: ReadonlyArray<unknown>) {
  const entries: LogEntry[] = [];
  for (const value of values) {
    const entry = yield* decodeStoredLogEntry(value);
    if (entry.seq !== entries.length)
      return yield* corrupt(
        `Invalid EventLog sequence: expected ${entries.length}, got ${entry.seq}`,
      );
    if (entry.seq === 0 ? entry._tag !== "session_started" : entry._tag === "session_started")
      return yield* corrupt("Fold log must start with exactly one session_started at seq 0");
    entries.push(entry);
  }
  return entries;
});
