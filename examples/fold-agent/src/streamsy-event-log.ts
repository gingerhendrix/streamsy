/** Fold owns agent semantics; the shared store owns the journal and producer log. */
import {
  decodeStoredLogEntry,
  EventLogCorruptEntryError,
  EventLogInvalidEntryError,
  EventLogUnsupportedVersionError,
  EventLogUnavailableError,
  eventLogSource,
  Ids,
  layerLiveIdFactory,
  LogEntry as LogEntrySchema,
  makeStoredLogEntry,
  type EventLogService,
  type LogEntryInput,
} from "@humanlayer/fold-core";
import { StreamUnavailable, Streams } from "@streamsy/core-next";
import { Context, Effect, Layer, Schema, Semaphore, Stream } from "effect";
import type { StreamsyStore } from "./storage.ts";
import {
  appendJournal,
  corrupt,
  decodeLog,
  readHistory,
  reconstruct,
  sessionRefs,
  settle,
  unavailable,
  type Pending,
} from "./session-journal.ts";

export type StreamsyEventLogMode = "create" | "resume" | "takeover";
export interface StreamsyEventLogOptions {
  readonly store: StreamsyStore;
  readonly streamId: string;
  readonly mode: StreamsyEventLogMode;
  /** An explicit resume epoch must match the durable journal; takeover increments it. */
  readonly epoch?: number;
}
const toFoldError = (error: { readonly _tag: string; readonly message?: string }) => {
  if (
    Schema.is(EventLogCorruptEntryError)(error) ||
    Schema.is(EventLogUnavailableError)(error) ||
    Schema.is(EventLogInvalidEntryError)(error) ||
    Schema.is(EventLogUnsupportedVersionError)(error)
  )
    return error;
  if (Schema.is(StreamUnavailable)(error))
    return unavailable(`Stream ${error.ref}: ${error.status}`);
  if (error._tag === "DecodeFault") return corrupt(error.message ?? "Invalid stored JSON");
  return unavailable(
    error.message || error._tag,
    "retryable" in error && error.retryable === true,
    error,
  );
};

/** Export the typed constructor for tests, avoiding casts through Fold's unknown-error descriptor. */
export const makeEventLog = (options: StreamsyEventLogOptions) =>
  Effect.gen(function* () {
    const refs = sessionRefs(options.streamId);
    const ids = Context.get(yield* Layer.build(layerLiveIdFactory), Ids);
    if (options.mode === "create") {
      for (const ref of [refs.log, refs.journal]) {
        const result = yield* Streams.create(ref);
        if (result.status !== "created")
          return yield* unavailable(
            `Stream ${ref.id} already exists or is unavailable: ${result.status}`,
          );
      }
      yield* appendJournal(
        refs,
        { _tag: "Epoch", epoch: 0, reason: "start" },
        (yield* readHistory(refs.journal)).offset,
      );
    }
    const history = yield* readHistory(refs.log);
    yield* decodeLog(history.items);
    const journal = yield* readHistory(refs.journal);
    const head = yield* reconstruct(journal.items, history.items);
    if (
      options.epoch !== undefined &&
      (!Number.isSafeInteger(options.epoch) || options.epoch !== head.epoch)
    )
      return yield* unavailable(
        `Fenced: requested epoch ${options.epoch} differs from journal epoch ${head.epoch}`,
      );

    // Settle OLD intent before claiming ownership or bumping the epoch. A competing
    // owner may settle the same tuple too; only one can win the following journal CAS.
    if (head.pending) yield* settle(refs, head.pending);
    let epoch = head.epoch;
    let nextProducerSeq = head.nextProducerSeq;
    let nextEntrySeq = head.nextEntrySeq;
    let journalOffset = journal.offset;
    if (options.mode !== "create") {
      if (nextEntrySeq === 0) return yield* unavailable("Cannot resume a log with no Fold entries");
      if (options.mode === "takeover") {
        if (!Number.isSafeInteger(epoch + 1)) return yield* corrupt("Epoch exhausted");
        epoch++;
        nextProducerSeq = 0;
      }
      journalOffset = yield* appendJournal(
        refs,
        { _tag: "Epoch", epoch, reason: options.mode },
        journalOffset,
      );
    }
    const lock = yield* Semaphore.make(1);
    // Set before the first interruptible operation. After failure/interruption the
    // caller must reconstruct; it cannot mint a replacement for uncertain intent.
    let uncertain = false;
    const append = Effect.fn("Fold.EventLog.append")((input: LogEntryInput) =>
      lock.withPermit(
        Effect.gen(function* () {
          if (uncertain)
            return yield* unavailable(
              "Pending outcome is uncertain; resume from the journal before appending",
            );
          if (
            nextEntrySeq === 0 ? input._tag !== "session_started" : input._tag === "session_started"
          )
            return yield* corrupt("Fold log requires exactly one initial session_started");
          const entry = yield* makeStoredLogEntry(input, nextEntrySeq, ids);
          // Preserve Fold v1 optional-field omission; toCodecJson would encode undefined as null.
          const bytes = yield* Schema.encodeEffect(Schema.fromJsonString(LogEntrySchema))(entry);
          const json = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(bytes);
          const pending: Pending = {
            _tag: "Pending",
            epoch,
            producerSeq: nextProducerSeq,
            entrySeq: nextEntrySeq,
            entryId: entry.eventId,
            entry: json,
          };
          uncertain = true;
          journalOffset = yield* appendJournal(refs, pending, journalOffset);
          yield* settle(refs, pending);
          nextEntrySeq++;
          nextProducerSeq++;
          uncertain = false;
          return entry;
        }).pipe(Effect.mapError(toFoldError)),
      ),
    );
    const entries: EventLogService["entries"] = (fromSeq = 0) =>
      Stream.unwrap(
        readHistory(refs.log).pipe(
          Effect.flatMap((log) => decodeLog(log.items)),
          Effect.map((log) => Stream.fromIterable(log.filter((entry) => entry.seq >= fromSeq))),
          Effect.mapError(toFoldError),
        ),
      ).pipe(Stream.provideContext(options.store.context));
    const subscribe: EventLogService["subscribe"] = (fromSeq = 0) =>
      Stream.unwrap(
        Effect.sync(() => {
          let seq = 0;
          return Streams.follow(refs.log).pipe(
            Streams.items,
            Stream.mapEffect((value) =>
              Effect.gen(function* () {
                const entry = yield* decodeStoredLogEntry(value);
                if (
                  entry.seq !== seq ||
                  (seq === 0 ? entry._tag !== "session_started" : entry._tag === "session_started")
                )
                  return yield* corrupt(`Invalid subscribed Fold sequence at ${seq}`);
                seq++;
                return entry;
              }),
            ),
            Stream.filter((entry) => entry.seq >= fromSeq),
            Stream.mapError(toFoldError),
          );
        }),
      ).pipe(Stream.provideContext(options.store.context));
    return {
      append: (input) => append(input).pipe(Effect.provide(options.store.context)),
      entries,
      subscribe,
    } satisfies EventLogService;
  }).pipe(Effect.provide(options.store.context), Effect.mapError(toFoldError));

export const streamsyEventLog = (options: StreamsyEventLogOptions) =>
  eventLogSource(makeEventLog(options));
export const readFoldLog = (store: StreamsyStore, streamId: string) =>
  readHistory(sessionRefs(streamId).log).pipe(
    Effect.flatMap((log) => decodeLog(log.items)),
    Effect.provide(store.context),
    Effect.mapError(toFoldError),
  );
