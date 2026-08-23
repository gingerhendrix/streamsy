/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow: every test callback is a Promise the runner awaits, and temp-dir plumbing is host wiring. The behaviour under test stays Effect values run with `Effect.runPromise`/`runPromiseExit`. */
/**
 * Contract tests for the Streamsy-backed Fold EventLog, run over both storage
 * backends. Each test names a claim the example makes about durability, and
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
  type EventLogError,
  type EventLogService,
  type LogEntry,
  type LogEntryInput,
} from "@humanlayer/fold-core";
import type { JsonValue } from "@streamsy/core";
import { StreamReadError } from "@streamsy/experimental/effect";
import { TestStreamsLayer } from "@streamsy/experimental/effect/testing";
import { Cause, Effect, Exit, Fiber, Stream, type Scope } from "effect";
import { openMemoryStore, openStore, type StreamsyStore } from "../src/storage.ts";
import {
  readFoldLog,
  streamsyEventLog,
  type StreamsyEventLogMode,
  type StreamsyEventLogOptions,
} from "../src/streamsy-event-log.ts";

const LONG_POLL_MS = 100;

/** The two backends the adapter must behave identically over. */
const backends: ReadonlyArray<{
  readonly name: string;
  readonly open: () => { readonly store: StreamsyStore; readonly dispose: () => Promise<void> };
}> = [
  {
    name: "memory",
    open: () => {
      const store = openMemoryStore({ longPollTimeoutMs: LONG_POLL_MS });
      return { store, dispose: () => store.close() };
    },
  },
  {
    name: "sqlite",
    open: () => {
      const dir = mkdtempSync(join(tmpdir(), "fold-streamsy-log-"));
      const store = openStore({
        filename: join(dir, "log.sqlite"),
        longPollTimeoutMs: LONG_POLL_MS,
      });
      return {
        store,
        dispose: async () => {
          await store.close();
          rmSync(dir, { recursive: true, force: true });
        },
      };
    },
  },
];

/** The `EventLogService` behind a descriptor, for tests that drive the port directly. */
const openLog = (
  store: StreamsyStore,
  streamId: string,
  mode: StreamsyEventLogMode,
  capabilities?: StreamsyEventLogOptions["capabilities"],
): Effect.Effect<EventLogService, EventLogError, Scope.Scope> => {
  const log = streamsyEventLog({
    binding: store.bind(streamId),
    mode,
    ...(capabilities === undefined ? {} : { capabilities }),
  });
  if (log._tag !== "source") throw new Error("expected a source-backed event log");
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion, effecttsgo/any-unknown-in-error-context, effecttsgo/unsafe-effect-type-assertion -- Fold's public seam types construction failures as `unknown`; this adapter only ever fails with `EventLogError`.
  return log.make as Effect.Effect<EventLogService, EventLogError, Scope.Scope>;
};

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
const rawTitleEntry = (seq: number): JsonValue => ({
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

/** Append arbitrary JSON through the client seam, bypassing the adapter, to plant fixtures. */
const plant = async (store: StreamsyStore, streamId: string, values: ReadonlyArray<JsonValue>) => {
  const handle = store.client.stream(streamId);
  const created = await handle.create({ contentType: "application/json" });
  if (created.status !== "created") throw new Error(`unexpected create: ${created.status}`);
  for (const value of values) {
    const appended = await handle.appendJsonBatch([value]);
    if (appended.status !== "appended") throw new Error(`unexpected append: ${appended.status}`);
  }
};

/** The typed error a failing adapter effect produced. */
const failureOf = <A, E>(exit: Exit.Exit<A, E>): { _tag?: string; message?: string } => {
  if (Exit.isSuccess(exit)) throw new Error("expected the effect to fail");
  const error = Cause.findErrorOption(exit.cause);
  if (error._tag === "None")
    throw new Error(`expected a typed failure: ${Cause.pretty(exit.cause)}`);
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Assertions only read optional `_tag`/`message` from the typed failure.
  return error.value as { _tag?: string; message?: string };
};

for (const backend of backends) {
  describe(`Streamsy EventLog (${backend.name})`, () => {
    const withStore = async <A>(run: (store: StreamsyStore) => Promise<A>): Promise<A> => {
      const opened = backend.open();
      try {
        return await run(opened.store);
      } finally {
        await opened.dispose();
      }
    };

    test("stores Fold entries as Streamsy messages that decode through Fold's own contract", async () => {
      await withStore(async (store) => {
        await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "log/a", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("first"));
            }),
          ),
        );

        // Read the raw JSON Streamsy actually persisted, then hand each value to
        // Fold's decoder. Nothing in this assertion trusts the adapter.
        const read = await store.client.stream("log/a").read();
        if (read.status !== "ok") throw new Error(`unexpected read: ${read.status}`);
        const stored: JsonValue[] = [];
        for await (const batch of read.session) {
          if (batch.kind !== "json") throw new Error(`unexpected batch kind: ${batch.kind}`);
          stored.push(...batch.items);
        }

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
        const entries = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "log/b", "create");
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
        await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "log/c", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));
            }),
          ),
        );

        const replayed = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "log/c", "resume");
              return yield* Stream.runCollect(log.entries(1));
            }),
          ),
        );

        expect(replayed.map((entry: LogEntry) => entry.seq)).toEqual([1, 2]);
      });
    });

    test("create mode refuses a stream that already exists", async () => {
      await withStore(async (store) => {
        await plant(store, "log/d", [rawTitleEntry(0)]);

        const exit = await Effect.runPromiseExit(Effect.scoped(openLog(store, "log/d", "create")));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("already exists");
      });
    });

    test("resume mode refuses a stream that does not exist", async () => {
      await withStore(async (store) => {
        const exit = await Effect.runPromiseExit(
          Effect.scoped(openLog(store, "log/missing", "resume")),
        );
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("not-found");
      });
    });

    test("resume mode refuses a log that does not start with session_started", async () => {
      await withStore(async (store) => {
        await plant(store, "log/e", [rawTitleEntry(0)]);

        const exit = await Effect.runPromiseExit(Effect.scoped(openLog(store, "log/e", "resume")));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogCorruptEntryError");
        expect(error.message).toContain("session_started");
      });
    });

    test("a sequence gap is a typed corruption failure, not a silent repair", async () => {
      await withStore(async (store) => {
        await plant(store, "log/f", [rawSessionStartedEntry(0), rawTitleEntry(2)]);

        const exit = await Effect.runPromiseExit(readFoldLog(store.bind("log/f")));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogCorruptEntryError");
        expect(error.message).toContain("expected 1, got 2");
      });
    });

    test("a non-entry JSON value is a typed corruption failure", async () => {
      await withStore(async (store) => {
        await plant(store, "log/g", ["not an entry"]);

        const exit = await Effect.runPromiseExit(readFoldLog(store.bind("log/g")));
        expect(failureOf(exit)._tag).toBe("EventLogCorruptEntryError");
      });
    });

    test("a competing writer is fenced by the exact-offset precondition", async () => {
      await withStore(async (store) => {
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const owner = yield* openLog(store, "log/h", "create");
              yield* owner.append(sessionStarted());

              // A second runtime adopts the same log at the same head...
              const rival = yield* openLog(store, "log/h", "resume");
              // ...the owner moves the log on...
              yield* owner.append(sessionTitle("owner wins the race"));
              // ...and the rival's stale head is rejected rather than re-sequenced.
              yield* rival.append(sessionTitle("rival is behind"));
            }),
          ),
        );

        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("Fenced");
      });
    });

    test("subscribe loses nothing across the catch-up/live boundary", async () => {
      await withStore(async (store) => {
        const collected = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "log/i", "create");
              // One entry exists before anyone subscribes: it must be replayed.
              yield* log.append(sessionStarted());

              const running = yield* Effect.forkChild(
                Stream.runCollect(log.subscribe().pipe(Stream.take(3))),
              );
              // These land after the subscription starts, some of them while the
              // first long poll is already in flight.
              yield* log.append(sessionTitle("live one"));
              yield* Effect.sleep(`${LONG_POLL_MS + 50} millis`);
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
        const collected = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "log/j", "create");
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
}

describe("Streamsy EventLog capability injection", () => {
  /**
   * The adapter consumes the `ReadStreams`/`AppendStreams` capabilities, so a
   * test can script them with `TestStreamsLayer` and no transport at all. Here
   * a scripted retryable read failure surfaces as Fold's typed unavailability.
   */
  test("a scripted capability failure surfaces as a typed Fold error", async () => {
    const store = openMemoryStore();
    const capabilities = TestStreamsLayer({
      read: {
        open: () =>
          Effect.fail(
            new StreamReadError({
              operation: "open",
              failure: { status: "error" },
              message: "scripted outage",
              code: "busy",
              retryable: true,
            }),
          ),
      },
      append: {
        append: () => Effect.die(new Error("unused")),
        appendJsonBatch: () => Effect.die(new Error("unused")),
      },
    });

    const exit = await Effect.runPromiseExit(
      Effect.scoped(openLog(store, "log/scripted", "resume", capabilities)),
    );
    await store.close();

    const error = failureOf(exit) as { _tag?: string; message?: string; retryable?: boolean };
    expect(error._tag).toBe("EventLogUnavailableError");
    expect(error.message).toContain("scripted outage");
    expect(error.retryable).toBe(true);
  });
});
