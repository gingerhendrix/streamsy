/** Fold owns agent semantics; the `Sessions` stream owns the durable event log. */
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
  storedLogEntry,
  type EventLogService,
  type LogEntryInput,
  type SessionId,
} from "@humanlayer/fold-core";
import {
  CreateResult,
  isStreamsFault,
  type ReadFailure,
  type FollowFailure,
  type AppendFailure,
  type CreateFailure,
} from "@streamsy/core";
import { Context, Effect, Layer, Match, Schema, Semaphore, Stream } from "effect";
import {
  Sessions,
  corrupt,
  decodeLog,
  isSessionStart,
  readHistory,
  unavailable,
  type SessionParams,
} from "./session-log.ts";

export type StreamsyEventLogMode = "create" | "resume";

export interface StreamsyEventLogOptions {
  readonly session: SessionId;
  readonly mode: StreamsyEventLogMode;
}

type LogFailure =
  | ReadFailure
  | FollowFailure
  | AppendFailure<{ expectedOffset: string }>
  | CreateFailure
  | Schema.SchemaError;

const toUnavailable = (error: LogFailure) =>
  unavailable(error.message, isStreamsFault(error) && error.retryable, error);

const toFoldError = (
  failure:
    | LogFailure
    | EventLogCorruptEntryError
    | EventLogUnavailableError
    | EventLogInvalidEntryError
    | EventLogUnsupportedVersionError,
) =>
  Match.valueTags(failure, {
    EventLogCorruptEntryError: (error) => error,
    EventLogUnavailableError: (error) => error,
    EventLogInvalidEntryError: (error) => error,
    EventLogUnsupportedVersionError: (error) => error,
    StreamNotFound: (error) => unavailable(`Stream ${error.id}: not-found`),
    StreamGone: (error) => unavailable(`Stream ${error.id}: gone`),
    DecodeFault: (error) => corrupt(error.message),
    TransportFault: toUnavailable,
    StorageFault: toUnavailable,
    EncodeFault: toUnavailable,
    InvalidReadRequest: toUnavailable,
    NotSupported: toUnavailable,
    StreamClosed: toUnavailable,
    StreamBusy: toUnavailable,
    AppendConflict: toUnavailable,
    OffsetMismatch: toUnavailable,
    CreateConflict: toUnavailable,
    ForkSourceNotFound: toUnavailable,
    InvalidForkRequest: toUnavailable,
    SchemaError: toUnavailable,
  });

/** Export the typed constructor for tests, avoiding casts through Fold's unknown-error descriptor. */
export const makeEventLog = (options: StreamsyEventLogOptions) =>
  Effect.gen(function* () {
    const sessions = yield* Sessions;
    const params: SessionParams = { session: options.session };
    const ids = Context.get(yield* Layer.build(layerLiveIdFactory), Ids);

    if (options.mode === "create") {
      const result = yield* sessions.create(params);

      if (CreateResult.$is("Exists")(result))
        return yield* unavailable(`Stream ${Sessions.address(params).id} already exists`);
    }

    const history = yield* readHistory(params);
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

          if (nextEntrySeq === 0 ? !isSessionStart(input) : isSessionStart(input))
            return yield* corrupt("Fold log requires exactly one initial session_started");
          const entry = yield* storedLogEntry(input, nextEntrySeq, yield* ids.makeEventId);
          // Preserve Fold v1 optional-field omission; toCodecJson would encode undefined as null.
          const bytes = yield* Schema.encodeEffect(Schema.fromJsonString(LogEntrySchema))(entry);
          const json = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(bytes);
          fenced = true;

          const result = yield* sessions
            .append(params, [json], { expectedOffset: tail })
            .pipe(
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
        readHistory(params).pipe(
          Effect.flatMap((log) => decodeLog(log.items)),
          Effect.map((log) => Stream.fromIterable(log.filter((entry) => entry.seq >= fromSeq))),
          Effect.mapError(toFoldError),
          Effect.provideService(Sessions, sessions),
        ),
      );

    const subscribe: EventLogService["subscribe"] = (fromSeq = 0) =>
      Stream.unwrap(
        Effect.sync(() => {
          let seq = 0;

          // `follow` catches up, then long-polls, and retries storage faults from the same offset.
          return sessions.follow(params).pipe(
            Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
            Stream.mapEffect((value) =>
              Effect.gen(function* () {
                const entry = yield* decodeStoredLogEntry(value);

                if (
                  entry.seq !== seq ||
                  (seq === 0 ? !isSessionStart(entry) : isSessionStart(entry))
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
      );

    return { append, entries, subscribe } satisfies EventLogService;
  }).pipe(Effect.mapError(toFoldError));

/** Fold's log descriptor over the `Sessions` service in context. */
export const streamsyEventLog = Effect.fn("Fold.streamsyEventLog")(function* (
  options: StreamsyEventLogOptions,
) {
  const sessions = yield* Sessions;

  return eventLogSource(makeEventLog(options).pipe(Effect.provideService(Sessions, sessions)));
});

/** Read and validate a stored session without a model. */
export const readFoldLog = (session: SessionId) =>
  readHistory({ session }).pipe(
    Effect.flatMap((log) => decodeLog(log.items)),
    Effect.mapError(toFoldError),
  );
