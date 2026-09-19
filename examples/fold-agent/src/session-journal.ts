/** Private Fold journal: durable intent before producer append; no acknowledgement rows. */
import {
  decodeStoredLogEntry,
  EventLogCorruptEntryError,
  EventLogUnavailableError,
  type LogEntry,
} from "@humanlayer/fold-core";
import { Producer, StreamRef, Streams, ZERO_OFFSET } from "@streamsy/core";
import { Effect, Schema, Stream } from "effect";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));
const Counter = Schema.Finite.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
export const JournalEntry = Schema.Union([
  Schema.TaggedStruct("Pending", {
    epoch: Counter,
    producerSeq: Counter,
    entrySeq: Counter,
    entryId: Schema.String,
    // Keep the encoded JSON, rather than decode/re-encode Fold's domain value on replay.
    entry: Schema.Json,
  }),
  Schema.TaggedStruct("Epoch", {
    epoch: Counter,
    reason: Schema.Literals(["start", "resume", "takeover"]),
  }),
]);
export type JournalEntry = typeof JournalEntry.Type;
export type Pending = Extract<JournalEntry, { _tag: "Pending" }>;
export const sessionRefs = (streamId: string) => {
  const match = /^fold\/sessions\/(.+)\/events$/.exec(streamId);
  if (!match?.[1]) throw new TypeError("Expected fold/sessions/<id>/events");
  return {
    log: StreamRef.json(streamId, { schema: Schema.Json }),
    journal: StreamRef.json(`fold/sessions/${match[1]}/journal`, { schema: JournalEntry }),
    producerId: `fold-session:${match[1]}`,
  };
};
export type SessionRefs = ReturnType<typeof sessionRefs>;
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

/** Validate all tuple/payload bindings, including earlier epochs, before replaying anything. */
export const reconstruct = Effect.fn("Fold.reconstruct")(function* (
  journal: ReadonlyArray<JournalEntry>,
  log: ReadonlyArray<Schema.Json>,
) {
  yield* decodeLog(log);
  let epoch = 0;
  let producerSeq = 0;
  let entrySeq = 0;
  let pending: Pending | undefined;
  let started = false;
  for (const row of journal) {
    if (row._tag === "Epoch") {
      if (!started) {
        if (row.reason !== "start" || row.epoch !== 0)
          return yield* corrupt("Journal must start at epoch 0");
        started = true;
      } else {
        if (pending) return yield* corrupt("Epoch advanced before pending settlement");
        const expected = row.reason === "takeover" ? epoch + 1 : epoch;
        if (row.reason === "start" || row.epoch !== expected)
          return yield* corrupt("Invalid journal epoch order");
      }
      if (row.epoch !== epoch) producerSeq = 0;
      epoch = row.epoch;
      continue;
    }
    if (
      !started ||
      pending ||
      row.epoch !== epoch ||
      row.producerSeq !== producerSeq ||
      row.entrySeq !== entrySeq
    )
      return yield* corrupt("Invalid journal pending order");
    const entry = yield* decodeStoredLogEntry(row.entry);
    if (entry.seq !== row.entrySeq || entry.eventId !== row.entryId)
      return yield* corrupt("Journal entry identity does not match payload");
    if (entrySeq === 0 ? entry._tag !== "session_started" : entry._tag === "session_started")
      return yield* corrupt("Journal must contain exactly one initial session_started");
    const saved = log[entrySeq];
    if (saved === undefined) pending = row;
    else if (encodeJson(saved) !== encodeJson(row.entry))
      return yield* corrupt("Journal payload differs from acknowledged log entry");
    producerSeq++;
    entrySeq++;
  }
  if (!started || log.length !== entrySeq - (pending ? 1 : 0))
    return yield* corrupt("Log and journal lengths disagree");
  return { epoch, nextProducerSeq: producerSeq, nextEntrySeq: entrySeq, pending };
});

/** Five total attempts, always the exact journaled tuple and encoded payload. */
export const settle = Effect.fn("Fold.settlePending")(function* (
  refs: SessionRefs,
  pending: Pending,
) {
  return yield* Producer.append(refs.log, [pending.entry], {
    producerId: refs.producerId,
    producerEpoch: pending.epoch,
    producerSeq: pending.producerSeq,
  }).pipe(
    Effect.retry({ times: 4, while: (error) => error._tag === "StorageFault" && error.retryable }),
    Effect.catchTags({
      StaleEpoch: () => unavailable("Fenced: stale-epoch"),
      ProducerGap: () => Effect.die(corrupt("Corrupt journal: producer-gap")),
      InvalidEpochSeq: () => Effect.die(corrupt("Corrupt journal: invalid-epoch-seq")),
      StreamBusy: () => unavailable("Streamsy log append: busy", true),
      StreamNotFound: appendUnavailable,
      StreamGone: appendUnavailable,
      StreamClosed: appendUnavailable,
      OffsetMismatch: appendUnavailable,
      AppendConflict: appendUnavailable,
      InvalidAppendRequest: appendUnavailable,
      NotSupported: appendUnavailable,
    }),
  );
});
const appendUnavailable = (error: { readonly _tag: string }) =>
  unavailable(`Streamsy log append: ${error._tag}`);

/** CAS is the ownership boundary. Never retry a journal append with a new tail. */
export const appendJournal = Effect.fn("Fold.appendJournal")(function* (
  refs: SessionRefs,
  row: JournalEntry,
  offset: string,
) {
  return yield* Streams.append(refs.journal, [row], { expectedOffset: offset }).pipe(
    Effect.map((result) => result.offset),
    Effect.catchTags({
      OffsetMismatch: () =>
        Effect.gen(function* () {
          const current = yield* readHistory(refs.journal);
          const epoch = current.items.findLast((item) => item._tag === "Epoch")?.epoch;
          return yield* unavailable(
            epoch !== undefined && epoch > row.epoch
              ? "Fenced: stale-epoch (journal owner advanced)"
              : "Fenced: journal expected-offset conflict",
          );
        }),
      StreamBusy: () => unavailable("Streamsy journal append: busy", true),
    }),
  );
});
