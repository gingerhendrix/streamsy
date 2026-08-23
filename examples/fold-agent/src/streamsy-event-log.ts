/**
 * A Fold Core `EventLog` backed by one Streamsy durable stream, written against
 * the Effect-native capabilities in `@streamsy/experimental/effect`.
 *
 * One Fold session is one JSON Streamsy stream: every Fold log entry is one
 * stored message, appended in Fold `seq` order. The two orderings stay separate
 * on purpose — Streamsy offsets are opaque transport checkpoints used for reads
 * and for the compare-and-swap precondition, Fold `seq` is the agent-domain
 * sequence carried inside the message. Neither is derived from the other.
 *
 * Unlike the Promise-bridging variant of this example, nothing here calls
 * `Effect.tryPromise`. Creation, reads, and appends go through the
 * `CreateStreams`, `ReadStreams`, and `AppendStreams` services. Their Live
 * layers own the Promise client, and their test layer can stand in for it
 * without any transport at all.
 *
 * The adapter deliberately stays inside this example: Fold is young, its log
 * API can still move, and no second consumer has yet justified a `@streamsy/*`
 * package.
 */
import {
  decodeStoredLogEntry,
  EventLogCorruptEntryError,
  EventLogUnavailableError,
  eventLogSource,
  Ids,
  layerLiveIdFactory,
  LogEntry as LogEntrySchema,
  makeStoredLogEntry,
  type EventLogError,
  type EventLogService,
  type FoldEventLog,
  type IdsService,
  type LogEntry,
  type LogEntryInput,
} from "@humanlayer/fold-core";
import type { JsonValue, StreamOffset } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import {
  AppendStreams,
  AppendStreamsLive,
  CreateStreams,
  CreateStreamsLive,
  ReadStreams,
  ReadStreamsLive,
  type AppendStreamsShape,
  type CreateStreamsShape,
  type ReadStreamsShape,
  type StreamCreateError,
  type StreamReadError,
} from "@streamsy/experimental/effect";
import { Context, Effect, Layer, Ref, Schema, Semaphore, Stream } from "effect";

/** How the adapter attaches to its Streamsy stream. */
export type StreamsyEventLogMode = "create" | "resume";

/** The capabilities the adapter consumes. Live by default, injectable in tests. */
export type StreamsyEventLogCapabilities = Layer.Layer<CreateStreams | ReadStreams | AppendStreams>;

export interface StreamsyEventLogOptions {
  /** The bound stream holding this session's Fold log. One stream is one session. */
  readonly binding: StreamBinding;
  /**
   * `create` requires a freshly created, empty stream; `resume` requires an
   * existing stream whose first entry is `session_started` at seq 0. The caller
   * states its intent — the adapter never guesses from current contents.
   */
  readonly mode: StreamsyEventLogMode;
  /**
   * The create/read/append capability layer to run against. Defaults to the
   * Live layers over the binding's client. Tests may supply
   * `TestStreamsLayer(...)` to script capability behaviour with no transport.
   */
  readonly capabilities?: StreamsyEventLogCapabilities;
}

const liveCapabilities: StreamsyEventLogCapabilities = Layer.mergeAll(
  CreateStreamsLive,
  ReadStreamsLive,
  AppendStreamsLive,
);

type Operation = "append" | "entries" | "subscribe";

const unavailable = (operation: Operation, message: string, retryable: boolean, cause?: unknown) =>
  new EventLogUnavailableError({
    operation,
    message,
    retryable,
    ...(cause === undefined ? {} : { cause }),
  });

const corrupt = (
  operation: "entries" | "subscribe",
  message: string,
  seq?: number,
  cause?: unknown,
) =>
  new EventLogCorruptEntryError({
    operation,
    message,
    ...(seq === undefined ? {} : { seq }),
    ...(cause === undefined ? {} : { cause }),
  });

/**
 * A capability failure becomes a Fold log error. A `parse-error` means the
 * stored bytes were not valid JSON — that is corruption, not unavailability.
 */
const fromReadError =
  (operation: "entries" | "subscribe") =>
  (error: StreamReadError): EventLogError =>
    error.code === "parse-error"
      ? corrupt(operation, "Stored Fold entry is not valid JSON", undefined, error)
      : unavailable(operation, error.message, error.retryable, error);

const fromCreateError = (error: StreamCreateError): EventLogError =>
  unavailable("append", error.message, error.retryable, error);

/**
 * Decode one stored Streamsy message into a Fold entry and check that it sits at
 * the sequence the log expects. A gap means the stream is not a coherent Fold
 * log, which is a corruption fact rather than something to paper over.
 */
const decodeEntryAt = (operation: "entries" | "subscribe", value: unknown, expectedSeq: number) =>
  Effect.gen(function* () {
    const entry = yield* decodeStoredLogEntry(value).pipe(
      Effect.mapError((error) =>
        error._tag === "EventLogCorruptEntryError"
          ? corrupt(operation, error.message, error.seq, error.cause)
          : error,
      ),
    );
    if (entry.seq !== expectedSeq) {
      return yield* corrupt(
        operation,
        `Invalid EventLog sequence: expected ${expectedSeq}, got ${entry.seq}`,
        entry.seq,
      );
    }
    return entry;
  });

/**
 * Drain one finite read session: every batch, decoded and sequence-checked,
 * plus the exact tail offset the drain ended on. The scope owns the session, so
 * failure or interruption cancels it exactly once.
 */
const readAll = (
  read: ReadStreamsShape,
  binding: StreamBinding,
  operation: "entries" | "subscribe",
): Effect.Effect<
  { readonly entries: ReadonlyArray<LogEntry>; readonly offset: StreamOffset },
  EventLogError
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const opened = yield* read.open(binding).pipe(Effect.mapError(fromReadError(operation)));
      if (opened.status !== "ok") {
        return yield* unavailable(
          operation,
          `Streamsy log stream "${binding.streamId}" is ${opened.status}`,
          false,
        );
      }

      const session = opened.session;
      const entries: LogEntry[] = [];
      let offset = session.startOffset ?? "";
      for (;;) {
        const result = yield* session.next.pipe(Effect.mapError(fromReadError(operation)));
        if (result.done) break;
        const batch = result.value;
        if (batch.kind !== "json") {
          return yield* corrupt(
            operation,
            `Stream "${binding.streamId}" delivered a ${batch.kind} batch; a Fold log is JSON`,
          );
        }
        for (const item of batch.items) {
          entries.push(yield* decodeEntryAt(operation, item, entries.length));
        }
        offset = batch.offset;
      }
      yield* session.done.pipe(Effect.mapError(fromReadError(operation)));
      return { entries, offset };
    }),
  );

const encodeEntry = (entry: LogEntry) =>
  Schema.encodeUnknownEffect(LogEntrySchema)(entry).pipe(
    Effect.mapError((cause) =>
      unavailable("append", "Unable to encode the Fold entry", false, cause),
    ),
  );

/** Create the durable stream for a fresh session through the Effect capability. */
const createLogStream = (
  create: CreateStreamsShape,
  binding: StreamBinding,
): Effect.Effect<void, EventLogError> =>
  Effect.gen(function* () {
    const created = yield* create
      .create(binding, { contentType: "application/json" })
      .pipe(Effect.mapError(fromCreateError));
    if (created.status === "conflict") {
      return yield* unavailable(
        "append",
        `Stream "${binding.streamId}" already exists; resume it instead of creating it`,
        false,
      );
    }
  });

/**
 * Build the `EventLogService` over the bound Streamsy stream.
 *
 * Everything durable happens before anything observable: an append is
 * constructed, encoded, and committed to Streamsy under an exact-offset
 * precondition, and only then does the cached head move.
 */
const makeService = (
  binding: StreamBinding,
  mode: StreamsyEventLogMode,
  ids: IdsService,
): Effect.Effect<EventLogService, EventLogError, CreateStreams | ReadStreams | AppendStreams> =>
  Effect.gen(function* () {
    const create: CreateStreamsShape = yield* CreateStreams;
    const read: ReadStreamsShape = yield* ReadStreams;
    const append: AppendStreamsShape = yield* AppendStreams;

    if (mode === "create") yield* createLogStream(create, binding);

    const initial = yield* readAll(read, binding, "entries");

    if (mode === "create" && initial.entries.length > 0) {
      return yield* unavailable(
        "append",
        `Stream "${binding.streamId}" is not empty; create mode requires a fresh log`,
        false,
      );
    }
    if (mode === "resume") {
      const first = initial.entries[0];
      if (first === undefined) {
        return yield* unavailable(
          "entries",
          `Stream "${binding.streamId}" holds no Fold entries`,
          false,
        );
      }
      if (first._tag !== "session_started") {
        return yield* corrupt(
          "entries",
          `Stream "${binding.streamId}" does not start with session_started`,
          first.seq,
        );
      }
    }

    // Cached head: the CAS precondition for the next append, plus the sequence
    // the next entry takes. Both only move after a durable append succeeds.
    const headRef = yield* Ref.make({ offset: initial.offset, nextSeq: initial.entries.length });
    const appendLock = yield* Semaphore.make(1);

    const appendEntry = Effect.fn("streamsy.event_log.append")((input: LogEntryInput) =>
      appendLock.withPermit(
        Effect.gen(function* () {
          const head = yield* Ref.get(headRef);
          const entry = yield* makeStoredLogEntry(input, head.nextSeq, ids);
          const encoded = yield* encodeEntry(entry);

          const result = yield* append
            // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- A schema-encoded Fold entry is a JSON object by construction; `LogEntry`'s encoded side has no wider static type.
            .appendJsonBatch(binding, [encoded as JsonValue], { expectedOffset: head.offset })
            .pipe(
              Effect.mapError((error) =>
                unavailable("append", error.message, error.retryable, error),
              ),
            );

          if (result.status === "conflict" && result.conflictReason === "expected-offset") {
            // Another writer moved the log while this runtime held state it had
            // already made decisions against. Fail loudly rather than silently
            // re-sequencing onto history this session has not seen.
            return yield* unavailable(
              "append",
              `Fenced: stream "${binding.streamId}" advanced to ${result.offset} while this session held ${head.offset}. Another writer owns this Fold session.`,
              false,
            );
          }
          if (result.status !== "appended") {
            return yield* unavailable(
              "append",
              `Streamsy append failed with status "${result.status}"`,
              false,
            );
          }

          yield* Ref.set(headRef, { offset: result.offset, nextSeq: entry.seq + 1 });
          return entry;
        }),
      ),
    );

    const entries: EventLogService["entries"] = (fromSeq = 0) =>
      Stream.unwrap(
        readAll(read, binding, "entries").pipe(
          Effect.map((log) =>
            Stream.fromIterable(log.entries.filter((entry) => entry.seq >= fromSeq)),
          ),
        ),
      );

    /**
     * Follow the durable stream itself rather than only this process's appends.
     * One live read session covers backlog and tail alike, so there is no
     * catch-up/live boundary to lose an entry across. The session is acquired
     * in the stream's own scope; interrupting the stream cancels it.
     */
    const subscribe: EventLogService["subscribe"] = (fromSeq = 0) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const opened = yield* read
            .open(binding, { live: "long-poll" })
            .pipe(Effect.mapError(fromReadError("subscribe")));
          if (opened.status !== "ok") {
            return yield* unavailable(
              "subscribe",
              `Streamsy log stream "${binding.streamId}" is ${opened.status}`,
              false,
            );
          }

          const session = opened.session;
          const seqRef = yield* Ref.make(0);
          const step = Effect.gen(function* () {
            const result = yield* session.next.pipe(Effect.mapError(fromReadError("subscribe")));
            if (result.done) {
              return yield* unavailable(
                "subscribe",
                `Streamsy log stream "${binding.streamId}" ended while a subscription was live`,
                false,
              );
            }
            const batch = result.value;
            if (batch.kind !== "json") {
              return yield* corrupt(
                "subscribe",
                `Stream "${binding.streamId}" delivered a ${batch.kind} batch; a Fold log is JSON`,
              );
            }
            const decoded: LogEntry[] = [];
            let nextSeq = yield* Ref.get(seqRef);
            for (const item of batch.items) {
              decoded.push(yield* decodeEntryAt("subscribe", item, nextSeq));
              nextSeq += 1;
            }
            yield* Ref.set(seqRef, nextSeq);
            return decoded.filter((entry) => entry.seq >= fromSeq);
          });

          return Stream.fromEffectRepeat(step).pipe(Stream.flattenIterable);
        }),
      );

    return { append: appendEntry, entries, subscribe };
  });

/**
 * Back one Fold session's durable log with one Streamsy stream.
 *
 * Plugs straight into `startSession({ log })` / `resumeSession({ log })`; Fold
 * keeps every bit of agent semantics, Streamsy keeps every bit of durability.
 */
export const streamsyEventLog = (options: StreamsyEventLogOptions): FoldEventLog =>
  eventLogSource(
    Effect.gen(function* () {
      const context = yield* Layer.build(layerLiveIdFactory);
      const ids = Context.get(context, Ids);
      return yield* makeService(options.binding, options.mode, ids).pipe(
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- The descriptor is the entry point Fold runs; it must hand Fold a self-contained `make` effect, so the capability layer is provided here.
        Effect.provide(options.capabilities ?? liveCapabilities),
      );
    }),
  );

/**
 * Read a session's durable Fold log straight out of Streamsy, without starting
 * a runtime or touching a provider. This is the honest inspection path: it
 * proves that what Streamsy stored decodes back through Fold's own wire
 * contract.
 */
export const readFoldLog = (
  binding: StreamBinding,
  capabilities?: Layer.Layer<ReadStreams>,
): Effect.Effect<ReadonlyArray<LogEntry>, EventLogError> =>
  Effect.gen(function* () {
    const read: ReadStreamsShape = yield* ReadStreams;
    const log = yield* readAll(read, binding, "entries");
    return log.entries;
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Inspection is its own entry point: it runs without any runtime or session and must be self-contained.
  }).pipe(Effect.provide(capabilities ?? ReadStreamsLive));
