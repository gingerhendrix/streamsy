/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- Bun owns Promise test callbacks and temporary SQLite fixture plumbing; effects run only at the test edge. */
/**
 * Contract tests for the Streamsy-backed Fold EventLog over a memory Layer. Each test names a claim the example makes about durability, and
 * asserts it against Fold's own wire contract rather than a local copy of it.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
  Schema,
  Stream,
} from "effect";
import { Sessions } from "../src/session-log.ts";
import { memoryLayer, sqliteLayer, StorageNotAvailable } from "../src/storage.ts";
import { readFoldLog, makeEventLog, type StreamsyEventLogMode } from "../src/streamsy-event-log.ts";
type JsonValue = Schema.Json;

/** One store for one test: a runtime over the store Layer, disposed at the end. */
type Store = ManagedRuntime.ManagedRuntime<Sessions, StorageFault | StorageNotAvailable>;

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
const plant = async (store: Store, name: string, values: ReadonlyArray<JsonValue>) => {
  const params = { session: sessionFor(name) };
  await store.runPromise(
    Effect.gen(function* () {
      expect((yield* (yield* Sessions).create(params))._tag).toBe("Created");
      for (const value of values)
        expect((yield* (yield* Sessions).append(params, [value]))._tag).toBe("Appended");
    }),
  );
};

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
  backend: string,
  open: () => {
    readonly layer: Layer.Layer<Sessions, StorageFault | StorageNotAvailable>;
    readonly cleanup: () => void;
  },
) =>
  describe(`Streamsy EventLog (${backend})`, () => {
    const withStore = async <A>(run: (store: Store) => Promise<A>): Promise<A> => {
      const { layer, cleanup } = open();
      const store = ManagedRuntime.make(layer);
      try {
        return await run(store);
      } finally {
        await store.dispose();
        cleanup();
      }
    };

    test("stores Fold entries as Streamsy messages that decode through Fold's own contract", async () => {
      await withStore(async (store) => {
        await store.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("a", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("first"));
            }),
          ),
        );

        // Read the raw JSON Streamsy actually persisted, then hand each value to
        // Fold's decoder. Nothing in this assertion trusts the adapter.
        const stored = await store.runPromise(
          Stream.unwrap(
            Effect.map(Sessions, (sessions) => sessions.read({ session: sessionFor("a") })),
          ).pipe(
            Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
            Stream.runCollect,
          ),
        );

        const decoded = await Effect.runPromise(
          Effect.forEach(stored, (value) => decodeStoredLogEntry(value)),
        );
        expect(decoded.map((entry) => entry._tag)).toEqual(["session_started", "session_title"]);
        expect(decoded.map((entry) => entry.seq)).toEqual([0, 1]);
        expect(decoded.every((entry) => entry.version === 1)).toBe(true);
      });
    });

    test("assigns contiguous Fold sequences independent of Streamsy offsets", async () => {
      await withStore(async (store) => {
        const entries = await store.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("b", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));
              return yield* Stream.runCollect(log.entries());
            }),
          ),
        );

        expect(entries.map((entry: LogEntry) => entry.seq)).toEqual([0, 1, 2]);
      });
    });

    test("entries(fromSeq) replays from durable storage and completes", async () => {
      await withStore(async (store) => {
        await store.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("c", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));
            }),
          ),
        );

        const replayed = await store.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("c", "resume");
              return yield* Stream.runCollect(log.entries(1));
            }),
          ),
        );

        expect(replayed.map((entry: LogEntry) => entry.seq)).toEqual([1, 2]);
      });
    });

    test("create mode refuses a stream that already exists", async () => {
      await withStore(async (store) => {
        await plant(store, "d", [rawTitleEntry(0)]);

        const exit = await store.runPromiseExit(Effect.scoped(openLog("d", "create")));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("already exists");
      });
    });

    test("resume mode refuses a stream that does not exist", async () => {
      await withStore(async (store) => {
        const exit = await store.runPromiseExit(Effect.scoped(openLog("missing", "resume")));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("not-found");
      });
    });

    test("resume mode refuses a log that does not start with session_started", async () => {
      await withStore(async (store) => {
        await plant(store, "e", [rawTitleEntry(0)]);

        const exit = await store.runPromiseExit(Effect.scoped(openLog("e", "resume")));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogCorruptEntryError");
        expect(error.message).toContain("session_started");
      });
    });

    test("a sequence gap is a typed corruption failure, not a silent repair", async () => {
      await withStore(async (store) => {
        await plant(store, "f", [rawSessionStartedEntry(0), rawTitleEntry(2)]);

        const exit = await store.runPromiseExit(readLog("f"));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogCorruptEntryError");
        expect(error.message).toContain("expected 1, got 2");
      });
    });

    test("a non-entry JSON value is a typed corruption failure", async () => {
      await withStore(async (store) => {
        await plant(store, "g", ["not an entry"]);

        const exit = await store.runPromiseExit(readLog("g"));
        expect(failureOf(exit)._tag).toBe("EventLogCorruptEntryError");
      });
    });

    test("a future Fold wire version preserves its typed unsupported-version error", async () => {
      await withStore(async (store) => {
        await plant(store, "future", [{ ...rawTitleEntry(0), version: 2 }]);
        const exit = await store.runPromiseExit(readLog("future"));
        expect(failureOf(exit)._tag).toBe("EventLogUnsupportedVersionError");
      });
    });

    test("a competing writer is fenced by the exact-offset precondition", async () => {
      await withStore(async (store) => {
        await store.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const owner = yield* openLog("h", "create");
              yield* owner.append(sessionStarted());

              // A second adapter reads the same tail.
              const rival = yield* openLog("h", "resume");
              // The rival moves the log on.
              yield* rival.append(sessionTitle("rival advances the log"));
              // The old owner cannot silently re-sequence onto new history.
              const conflict = yield* Effect.flip(
                owner.append(sessionTitle("old owner is behind")),
              );
              expect(conflict._tag).toBe("EventLogUnavailableError");
              expect(conflict.message).toContain("Fenced: another writer advanced the log");

              const latched = yield* Effect.flip(owner.append(sessionTitle("still fenced")));
              expect(latched.message).toContain("This writer is fenced");
            }),
          ),
        );
      });
    });

    test("subscribe loses nothing across the catch-up/live boundary", async () => {
      await withStore(async (store) => {
        const collected = await store.runPromise(
          Effect.scoped(
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
          ),
        );

        expect(collected.map((entry: LogEntry) => entry.seq)).toEqual([0, 1, 2]);
        expect(collected.map((entry: LogEntry) => entry._tag)).toEqual([
          "session_started",
          "session_title",
          "session_title",
        ]);
      });
    }, 20_000);

    test("subscribe(fromSeq) skips entries before the requested sequence", async () => {
      await withStore(async (store) => {
        const collected = await store.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog("j", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));

              return yield* Stream.runCollect(log.subscribe(2).pipe(Stream.take(1)));
            }),
          ),
        );

        expect(collected.map((entry: LogEntry) => entry.seq)).toEqual([2]);
      });
    }, 20_000);
  });

registerEventLogContract("memory", () => ({ layer: memoryLayer(), cleanup: () => {} }));
registerEventLogContract("SQLite", () => {
  const dir = mkdtempSync(join(tmpdir(), "fold-streamsy-contract-"));
  return {
    layer: sqliteLayer(join(dir, "event-log.sqlite"), { longPollTimeoutMs: 25 }),
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
});

describe("Fold writer capability failures", () => {
  for (const operation of ["read", "create"] as const) {
    test(`a scripted ${operation} capability failure remains typed and retryable`, async () => {
      await Effect.runPromise(
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
              ? { read: () => Stream.fail(fault) }
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
        }).pipe(Effect.provide(memoryLayer())),
      );
    });
  }
});
