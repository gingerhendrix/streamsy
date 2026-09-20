/** Fold owns agent semantics; the shared store owns the durable event log. */
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
import { StreamNotFound, StreamGone, Streams } from "@streamsy/core";
import { Context, Effect, Layer, Schema, Semaphore, Stream } from "effect";
import type { StreamsyStore } from "./storage.ts";
import { corrupt, decodeLog, readHistory, sessionRef, unavailable } from "./session-log.ts";

export type StreamsyEventLogMode = "create" | "resume";
export interface StreamsyEventLogOptions {
  readonly store: StreamsyStore;
  readonly streamId: string;
  readonly mode: StreamsyEventLogMode;
}
const toFoldError = (error: { readonly _tag: string; readonly message?: string }) => {
  if (
    Schema.is(EventLogCorruptEntryError)(error) ||
    Schema.is(EventLogUnavailableError)(error) ||
    Schema.is(EventLogInvalidEntryError)(error) ||
    Schema.is(EventLogUnsupportedVersionError)(error)
  )
    return error;
  if (Schema.is(StreamNotFound)(error)) return unavailable(`Stream ${error.id}: not-found`);
  if (Schema.is(StreamGone)(error)) return unavailable(`Stream ${error.id}: gone`);
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
    const ref = sessionRef(options.streamId);
    const ids = Context.get(yield* Layer.build(layerLiveIdFactory), Ids);
    if (options.mode === "create") {
      const result = yield* Streams.create(ref);
      if (result._tag === "Exists")
        return yield* unavailable(
          `Stream ${ref.id} already exists or is unavailable: ${result._tag}`,
        );
    }
    const history = yield* readHistory(ref);
    const decoded = yield* decodeLog(history.items);
    if (options.mode === "resume" && decoded.length === 0)
      return yield* unavailable("Cannot resume a log with no Fold entries");

    let tail = history.offset;
    let nextEntrySeq = decoded.length;
    const lock = yield* Semaphore.make(1);
    let fenced = false;
    const append = Effect.fn("Fold.EventLog.append")((input: LogEntryInput) =>
      lock.withPermit(
        Effect.gen(function* () {
          if (fenced) return yield* unavailable("This writer is fenced; resume from the log");
          if (
            nextEntrySeq === 0 ? input._tag !== "session_started" : input._tag === "session_started"
          )
            return yield* corrupt("Fold log requires exactly one initial session_started");
          const entry = yield* makeStoredLogEntry(input, nextEntrySeq, ids);
          // Preserve Fold v1 optional-field omission; toCodecJson would encode undefined as null.
          const bytes = yield* Schema.encodeEffect(Schema.fromJsonString(LogEntrySchema))(entry);
          const json = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(bytes);
          fenced = true;
          const result = yield* Streams.append(ref, [json], { expectedOffset: tail }).pipe(
            Effect.catchTag("OffsetMismatch", () =>
              unavailable("Fenced: another writer advanced the log"),
            ),
          );
          tail = result.offset;
          nextEntrySeq++;
          fenced = false;
          return entry;
        }).pipe(Effect.mapError(toFoldError)),
      ),
    );
    const entries: EventLogService["entries"] = (fromSeq = 0) =>
      Stream.unwrap(
        readHistory(ref).pipe(
          Effect.flatMap((log) => decodeLog(log.items)),
          Effect.map((log) => Stream.fromIterable(log.filter((entry) => entry.seq >= fromSeq))),
          Effect.mapError(toFoldError),
        ),
      ).pipe(Stream.provideContext(options.store.context));
    const subscribe: EventLogService["subscribe"] = (fromSeq = 0) =>
      Stream.unwrap(
        Effect.sync(() => {
          let seq = 0;
          return Streams.follow(ref).pipe(
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
  readHistory(sessionRef(streamId)).pipe(
    Effect.flatMap((log) => decodeLog(log.items)),
    Effect.provide(store.context),
    Effect.mapError(toFoldError),
  );
