import {
  decodeStoredLogEntry,
  EventLogCorruptEntryError,
  EventLogUnavailableError,
  SessionId,
  type LogEntry,
} from "@humanlayer/fold-core";
import { Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Effect, Predicate, Schema, Stream } from "effect";

/** One JSON stream for each Fold session, `fold/sessions/<session>/events`. */
export class Sessions extends Streamsy.json<Sessions>()(
  "Fold/Sessions",
  "fold/sessions/:session/events",
  { params: { session: SessionId }, schema: Schema.Json },
) {}

export interface SessionParams {
  readonly session: SessionId;
}

/** The stream id that the CLI prints and accepts. */
export const sessionStreamId = (session: SessionId): string => Sessions.address({ session }).id;

export const unavailable = (message: string, retryable = false, cause?: unknown) =>
  new EventLogUnavailableError({
    operation: "append",
    message,
    retryable,
    ...(cause === undefined ? {} : { cause }),
  });

export const corrupt = (message: string) =>
  new EventLogCorruptEntryError({ operation: "entries", message });

export const readHistory = Effect.fn("Fold.readHistory")(function* (params: SessionParams) {
  const batches = yield* Stream.runCollect((yield* Sessions).read(params));
  return {
    items: batches.flatMap((batch) => batch.items),
    offset: batches.at(-1)?.nextOffset ?? ZERO_OFFSET,
  };
});

/** Fold requires exactly one `session_started` entry, and it must be the first. */
export const isSessionStart = Predicate.isTagged("session_started");

export const decodeLog = Effect.fn("Fold.decodeLog")(function* (values: ReadonlyArray<unknown>) {
  const entries: LogEntry[] = [];
  for (const value of values) {
    const entry = yield* decodeStoredLogEntry(value);
    if (entry.seq !== entries.length)
      return yield* corrupt(
        `Invalid EventLog sequence: expected ${entries.length}, got ${entry.seq}`,
      );
    if (entry.seq === 0 ? !isSessionStart(entry) : isSessionStart(entry))
      return yield* corrupt("Fold log must start with exactly one session_started at seq 0");
    entries.push(entry);
  }
  return entries;
});
