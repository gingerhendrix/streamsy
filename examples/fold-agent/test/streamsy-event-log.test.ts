/**
 * Contract tests for the Streamsy-backed Fold EventLog over a memory Layer. Each test names a claim the example makes about durability, and
 * asserts it against Fold's own wire contract rather than a local copy of it.
 */
import { BunServices } from "@effect/platform-bun";
import { layer, type Vitest } from "@effect/vitest";
import { describe, expect } from "vitest";
import {
  AgentId,
  decodeStoredLogEntry,
  EventId,
  SessionId,
  type LogEntry,
  type LogEntryInput,
} from "@humanlayer/fold-core";
import { StorageFault } from "@streamsy/core";
import {
  FileSystem,
  Path,
  PlatformError,
  Scope,
  Context,
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Schema,
  Stream,
} from "effect";
import { Sessions } from "../src/session-log.ts";
import { memoryLayer, sqliteLayer } from "../src/storage.ts";
import { readFoldLog, makeEventLog, type StreamsyEventLogMode } from "../src/streamsy-event-log.ts";

type JsonValue = Schema.Json;

/** One store for one test: a runtime over the store Layer, disposed at the end. */
type Store = Context.Context<Sessions>;

/** Each test names its session. The name maps to one real Fold session id. */
const sessionIds = new Map<string, SessionId>();

const sessionFor = (name: string): SessionId => {
  const known = sessionIds.get(name);

  if (known !== undefined) return known;
  const created = SessionId.create();
  sessionIds.set(name, created);

  return created;
};

const openLog = (name: string, mode: StreamsyEventLogMode) =>
  makeEventLog({ session: sessionFor(name), mode });

const readLog = (name: string) => readFoldLog(sessionFor(name));

const sessionStarted = (): LogEntryInput => ({
  _tag: "session_started",
  agentId: null,
  parentAgentId: null,
  toolCallId: null,
  cwd: null,
  sessionId: SessionId.create(),
  rootAgentId: AgentId.create(),
  meta: {},
});

const sessionTitle = (title: string): LogEntryInput => ({
  _tag: "session_title",
  agentId: null,
  parentAgentId: null,
  toolCallId: null,
  title,
});

/** A hand-built persisted entry, for planting exactly the corruption under test. */
const rawTitleEntry = (seq: number) => ({
  _tag: "session_title",
  seq,
  eventId: EventId.create(),
  ts: 1_700_000_000_000,
  version: 1,
  agentId: null,
  parentAgentId: null,
  toolCallId: null,
  title: `raw-${seq}`,
});

const rawSessionStartedEntry = (seq: number): JsonValue => ({
  _tag: "session_started",
  seq,
  eventId: EventId.create(),
  ts: 1_700_000_000_000,
  version: 1,
  agentId: null,
  parentAgentId: null,
  toolCallId: null,
  cwd: null,
  sessionId: SessionId.create(),
  rootAgentId: AgentId.create(),
  meta: {},
});

/** Append arbitrary JSON through the stream service, bypassing the adapter, to plant fixtures. */
const plant = (store: Store, name: string, values: ReadonlyArray<JsonValue>) =>
  Effect.gen(function* () {
    const params = { session: sessionFor(name) };
    yield* Effect.gen(function* () {
      expect((yield* (yield* Sessions).create(params))._tag).toBe("Created");

      for (const value of values)
        expect((yield* (yield* Sessions).append(params, [value]))._tag).toBe("Appended");
    }).pipe(Effect.provideContext(store));
  });

/** The typed error a failing adapter effect produced. */
const failureOf = <A, E>(exit: Exit.Exit<A, E>): { _tag?: string; message?: string } => {
  if (Exit.isSuccess(exit)) throw new Error("expected the effect to fail");
  const error = Cause.findErrorOption(exit.cause);

  if (error._tag === "None")
    throw new Error(`expected a typed failure: ${Cause.pretty(exit.cause)}`);

  return Schema.decodeUnknownSync(
    Schema.Struct({
      _tag: Schema.optionalKey(Schema.String),
      message: Schema.optionalKey(Schema.String),
    }),
  )(error.value);
};

const registerEventLogContract = (
  it: Vitest.MethodsNonLive<BunServices.BunServices>,
  backend: string,
  open: () => Effect.Effect<
    Layer.Layer<Sessions, StorageFault>,
    PlatformError.PlatformError,
    FileSystem.FileSystem | Path.Path | Scope.Scope
  >,
) => {
  const withStore = <A, E, R>(run: (store: Store) => Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const fixture = yield* open();
      const store = yield* Layer.build(fixture);

      return yield* run(store);
    });

  describe(`Streamsy EventLog (${backend})`, () => {
    it.effect(
      "stores Fold entries as Streamsy messages that decode through Fold's own contract",
      () =>
        withStore((store) =>
          Effect.gen(function* () {
            yield* Effect.scoped(
              Effect.gen(function* () {
                const log = yield* openLog("a", "create");
                yield* log.append(sessionStarted());
                yield* log.append(sessionTitle("first"));
              }),
            ).pipe(Effect.provideContext(store));

            // Read the raw JSON Streamsy actually persisted, then hand each value to
            // Fold's decoder. Nothing in this assertion trusts the adapter.
            const stored = yield* Stream.unwrap(
              Effect.map(Sessions, (sessions) => sessions.read({ session: sessionFor("a") })),
            ).pipe(
              Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
              Stream.runCollect,
              Effect.provideContext(store),
            );

            const decoded = yield* Effect.forEach(stored, (value) => decodeStoredLogEntry(value));
            expect(decoded.map((entry) => entry._tag)).toEqual([
              "session_started",
              "session_title",
            ]);
            expect(decoded.map((entry) => entry.seq)).toEqual([0, 1]);
            expect(decoded.every((entry) => entry.version === 1)).toBe(true);
          }),
        ).pipe(Effect.asVoid),
    );

    it.effect("assigns contiguous Fold sequences independent of Streamsy offsets", () =>
      withStore((store) =>
        Effect.gen(function* () {
          const entries = yield* Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("b", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));

              return yield* Stream.runCollect(log.entries());
            }),
          ).pipe(Effect.provideContext(store));

          expect(entries.map((entry: LogEntry) => entry.seq)).toEqual([0, 1, 2]);
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("entries(fromSeq) replays from durable storage and completes", () =>
      withStore((store) =>
        Effect.gen(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("c", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));
            }),
          ).pipe(Effect.provideContext(store));

          const replayed = yield* Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("c", "resume");

              return yield* Stream.runCollect(log.entries(1));
            }),
          ).pipe(Effect.provideContext(store));

          expect(replayed.map((entry: LogEntry) => entry.seq)).toEqual([1, 2]);
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("create mode refuses a stream that already exists", () =>
      withStore((store) =>
        Effect.gen(function* () {
          yield* plant(store, "d", [rawTitleEntry(0)]);

          const exit = yield* Effect.scoped(openLog("d", "create")).pipe(
            Effect.provideContext(store),
            Effect.exit,
          );

          const error = failureOf(exit);
          expect(error._tag).toBe("EventLogUnavailableError");
          expect(error.message).toContain("already exists");
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("resume mode refuses a stream that does not exist", () =>
      withStore((store) =>
        Effect.gen(function* () {
          const exit = yield* Effect.scoped(openLog("missing", "resume")).pipe(
            Effect.provideContext(store),
            Effect.exit,
          );

          const error = failureOf(exit);
          expect(error._tag).toBe("EventLogUnavailableError");
          expect(error.message).toContain("not-found");
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("resume mode refuses a log that does not start with session_started", () =>
      withStore((store) =>
        Effect.gen(function* () {
          yield* plant(store, "e", [rawTitleEntry(0)]);

          const exit = yield* Effect.scoped(openLog("e", "resume")).pipe(
            Effect.provideContext(store),
            Effect.exit,
          );

          const error = failureOf(exit);
          expect(error._tag).toBe("EventLogCorruptEntryError");
          expect(error.message).toContain("session_started");
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("a sequence gap is a typed corruption failure, not a silent repair", () =>
      withStore((store) =>
        Effect.gen(function* () {
          yield* plant(store, "f", [rawSessionStartedEntry(0), rawTitleEntry(2)]);

          const exit = yield* readLog("f").pipe(Effect.provideContext(store), Effect.exit);
          const error = failureOf(exit);
          expect(error._tag).toBe("EventLogCorruptEntryError");
          expect(error.message).toContain("expected 1, got 2");
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("a non-entry JSON value is a typed corruption failure", () =>
      withStore((store) =>
        Effect.gen(function* () {
          yield* plant(store, "g", ["not an entry"]);

          const exit = yield* readLog("g").pipe(Effect.provideContext(store), Effect.exit);
          expect(failureOf(exit)._tag).toBe("EventLogCorruptEntryError");
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("a future Fold wire version preserves its typed unsupported-version error", () =>
      withStore((store) =>
        Effect.gen(function* () {
          yield* plant(store, "future", [{ ...rawTitleEntry(0), version: 2 }]);
          const exit = yield* readLog("future").pipe(Effect.provideContext(store), Effect.exit);
          expect(failureOf(exit)._tag).toBe("EventLogUnsupportedVersionError");
        }),
      ).pipe(Effect.asVoid),
    );

    it.effect("a competing writer is fenced by the exact-offset precondition", () =>
      withStore((store) =>
        Effect.scoped(
          Effect.gen(function* () {
            const owner = yield* openLog("h", "create");
            yield* owner.append(sessionStarted());

            // A second adapter reads the same tail.
            const rival = yield* openLog("h", "resume");
            // The rival moves the log on.
            yield* rival.append(sessionTitle("rival advances the log"));

            // The old owner cannot silently re-sequence onto new history.
            const conflict = yield* Effect.flip(owner.append(sessionTitle("old owner is behind")));

            expect(conflict._tag).toBe("EventLogUnavailableError");
            expect(conflict.message).toContain("Fenced: another writer advanced the log");

            const latched = yield* Effect.flip(owner.append(sessionTitle("still fenced")));
            expect(latched.message).toContain("This writer is fenced");
          }),
        ).pipe(Effect.provideContext(store), Effect.asVoid),
      ).pipe(Effect.asVoid),
    );

    it.effect(
      "subscribe loses nothing across the catch-up/live boundary",
      () =>
        withStore((store) =>
          Effect.gen(function* () {
            const collected = yield* Effect.scoped(
              Effect.gen(function* () {
                const log = yield* openLog("i", "create");
                // One entry exists before anyone subscribes: it must be replayed.
                yield* log.append(sessionStarted());

                const observed = yield* Deferred.make<void>();

                const running = yield* Effect.forkChild(
                  Stream.runCollect(
                    log.subscribe().pipe(
                      Stream.tap((entry) =>
                        entry.seq === 1 ? Deferred.succeed(observed, undefined) : Effect.void,
                      ),
                      Stream.take(3),
                    ),
                  ),
                );

                // These land after the subscription starts, some of them while the
                // first long poll is already in flight.
                yield* log.append(sessionTitle("live one"));
                yield* Deferred.await(observed);
                yield* log.append(sessionTitle("live two"));

                return yield* Fiber.join(running);
              }),
            ).pipe(Effect.provideContext(store));

            expect(collected.map((entry: LogEntry) => entry.seq)).toEqual([0, 1, 2]);
            expect(collected.map((entry: LogEntry) => entry._tag)).toEqual([
              "session_started",
              "session_title",
              "session_title",
            ]);
          }),
        ).pipe(Effect.asVoid),
      20_000,
    );

    it.effect(
      "subscribe(fromSeq) skips entries before the requested sequence",
      () =>
        withStore((store) =>
          Effect.gen(function* () {
            const collected = yield* Effect.scoped(
              Effect.gen(function* () {
                const log = yield* openLog("j", "create");
                yield* log.append(sessionStarted());
                yield* log.append(sessionTitle("one"));
                yield* log.append(sessionTitle("two"));

                return yield* Stream.runCollect(log.subscribe(2).pipe(Stream.take(1)));
              }),
            ).pipe(Effect.provideContext(store));

            expect(collected.map((entry: LogEntry) => entry.seq)).toEqual([2]);
          }),
        ).pipe(Effect.asVoid),
      20_000,
    );
  });
};

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  registerEventLogContract(it, "memory", () => Effect.succeed(memoryLayer()));
  registerEventLogContract(it, "SQLite", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "fold-streamsy-contract-" });

      return sqliteLayer(path.join(dir, "event-log.sqlite"), { longPollTimeoutMs: 25 });
    }),
  );

  describe("Fold writer capability failures", () => {
    for (const operation of ["read", "create"] as const) {
      it.effect(`a scripted ${operation} capability failure remains typed and retryable`, () =>
        Effect.gen(function* () {
          const fault = new StorageFault({
            operation,
            message: "scripted outage",
            retryable: true,
          });

          const sessions = yield* Sessions;

          const failing = {
            ...sessions,
            ...(operation === "read"
              ? { collect: () => Effect.fail(fault) }
              : { create: () => Effect.fail(fault) }),
          };

          const failure = yield* Effect.flip(
            Effect.scoped(openLog("recovery", operation === "read" ? "resume" : "create")).pipe(
              Effect.provideService(Sessions, failing),
            ),
          );

          expect(failure).toMatchObject({
            _tag: "EventLogUnavailableError",
            message: "scripted outage",
            retryable: true,
          });
        }).pipe(withLayer(memoryLayer()), Effect.asVoid),
      );
    }
  });
});

/** Build local fixtures in a scope and supply their Context to the program. */
const withLayer =
  <ROut, LE, RIn>(fixture: Layer.Layer<ROut, LE, RIn>) =>
  <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(fixture);

        return yield* program.pipe(Effect.provideContext(services));
      }),
    );
