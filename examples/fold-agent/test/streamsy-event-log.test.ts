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
import {
  StorageFault,
  Memory,
  Protocol,
  Producer,
  StreamsReader,
  StreamsWriter,
  Streams,
  StreamRef,
} from "@streamsy/core";
import { Context, Deferred, Layer, Schema } from "effect";
import { faultyStorage } from "@streamsy/core/testing";
import { appendJournal, readHistory, sessionRefs, type Pending } from "../src/session-journal.ts";
type JsonValue = Schema.Json;
import { Cause, Effect, Exit, Fiber, Stream } from "effect";
import {
  openMemoryStore,
  openStore,
  StorageNotAvailable,
  type StreamsyStore,
} from "../src/storage.ts";
import { readFoldLog, makeEventLog, type StreamsyEventLogMode } from "../src/streamsy-event-log.ts";

const openLog = (store: StreamsyStore, streamId: string, mode: StreamsyEventLogMode) =>
  makeEventLog({ store, streamId, mode });

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

/** Append arbitrary JSON through the client seam, bypassing the adapter, to plant fixtures. */
const plant = async (store: StreamsyStore, streamId: string, values: ReadonlyArray<JsonValue>) => {
  const ref = StreamRef.json(streamId, { schema: Schema.Json });
  await Effect.runPromise(
    Effect.gen(function* () {
      expect((yield* Streams.create(ref)).status).toBe("created");
      for (const value of values)
        expect((yield* Streams.append(ref, [value])).status).toBe("appended");
    }).pipe(Effect.provide(store.context)),
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
  open: () => Effect.Effect<StreamsyStore, StorageFault | StorageNotAvailable>,
) =>
  describe(`Streamsy EventLog (${backend})`, () => {
    const withStore = async <A>(run: (store: StreamsyStore) => Promise<A>): Promise<A> => {
      const store = await Effect.runPromise(open());
      try {
        return await run(store);
      } finally {
        await store.close();
      }
    };

    test("stores Fold entries as Streamsy messages that decode through Fold's own contract", async () => {
      await withStore(async (store) => {
        await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "fold/sessions/a/events", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("first"));
            }),
          ),
        );

        // Read the raw JSON Streamsy actually persisted, then hand each value to
        // Fold's decoder. Nothing in this assertion trusts the adapter.
        const stored = await Effect.runPromise(
          Streams.read(StreamRef.json("fold/sessions/a/events", { schema: Schema.Json })).pipe(
            Streams.items,
            Stream.runCollect,
            Effect.provide(store.context),
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
        const entries = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "fold/sessions/b/events", "create");
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
              const log = yield* openLog(store, "fold/sessions/c/events", "create");
              yield* log.append(sessionStarted());
              yield* log.append(sessionTitle("one"));
              yield* log.append(sessionTitle("two"));
            }),
          ),
        );

        const replayed = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "fold/sessions/c/events", "resume");
              return yield* Stream.runCollect(log.entries(1));
            }),
          ),
        );

        expect(replayed.map((entry: LogEntry) => entry.seq)).toEqual([1, 2]);
      });
    });

    test("create mode refuses a stream that already exists", async () => {
      await withStore(async (store) => {
        await plant(store, "fold/sessions/d/events", [rawTitleEntry(0)]);

        const exit = await Effect.runPromiseExit(
          Effect.scoped(openLog(store, "fold/sessions/d/events", "create")),
        );
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("already exists");
      });
    });

    test("resume mode refuses a stream that does not exist", async () => {
      await withStore(async (store) => {
        const exit = await Effect.runPromiseExit(
          Effect.scoped(openLog(store, "fold/sessions/missing/events", "resume")),
        );
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogUnavailableError");
        expect(error.message).toContain("not-found");
      });
    });

    test("resume mode refuses a log that does not start with session_started", async () => {
      await withStore(async (store) => {
        await plant(store, "fold/sessions/e/events", [rawTitleEntry(0)]);

        const exit = await Effect.runPromiseExit(
          Effect.scoped(openLog(store, "fold/sessions/e/events", "resume")),
        );
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogCorruptEntryError");
        expect(error.message).toContain("session_started");
      });
    });

    test("a sequence gap is a typed corruption failure, not a silent repair", async () => {
      await withStore(async (store) => {
        await plant(store, "fold/sessions/f/events", [rawSessionStartedEntry(0), rawTitleEntry(2)]);

        const exit = await Effect.runPromiseExit(readFoldLog(store, "fold/sessions/f/events"));
        const error = failureOf(exit);
        expect(error._tag).toBe("EventLogCorruptEntryError");
        expect(error.message).toContain("expected 1, got 2");
      });
    });

    test("a non-entry JSON value is a typed corruption failure", async () => {
      await withStore(async (store) => {
        await plant(store, "fold/sessions/g/events", ["not an entry"]);

        const exit = await Effect.runPromiseExit(readFoldLog(store, "fold/sessions/g/events"));
        expect(failureOf(exit)._tag).toBe("EventLogCorruptEntryError");
      });
    });

    test("a future Fold wire version preserves its typed unsupported-version error", async () => {
      await withStore(async (store) => {
        await plant(store, "fold/sessions/future/events", [{ ...rawTitleEntry(0), version: 2 }]);
        const exit = await Effect.runPromiseExit(readFoldLog(store, "fold/sessions/future/events"));
        expect(failureOf(exit)._tag).toBe("EventLogUnsupportedVersionError");
      });
    });

    test("a competing writer is fenced by the exact-offset precondition", async () => {
      await withStore(async (store) => {
        const exit = await Effect.runPromiseExit(
          Effect.scoped(
            Effect.gen(function* () {
              const owner = yield* openLog(store, "fold/sessions/h/events", "create");
              yield* owner.append(sessionStarted());

              // A second adapter claims the journal at the same producer epoch.
              const rival = yield* openLog(store, "fold/sessions/h/events", "resume");
              // The new journal owner moves the log on.
              yield* rival.append(sessionTitle("rival owns the journal"));
              // The old owner cannot silently re-sequence onto new history.
              yield* owner.append(sessionTitle("old owner is behind"));
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
              const log = yield* openLog(store, "fold/sessions/i/events", "create");
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
        const collected = await Effect.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const log = yield* openLog(store, "fold/sessions/j/events", "create");
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

registerEventLogContract("memory", openMemoryStore);
registerEventLogContract("SQLite", () => {
  const dir = mkdtempSync(join(tmpdir(), "fold-streamsy-contract-"));
  return openStore({ filename: join(dir, "event-log.sqlite"), longPollTimeoutMs: 25 }).pipe(
    Effect.map((store) => ({
      ...store,
      close: async () => {
        await store.close();
        rmSync(dir, { recursive: true, force: true });
      },
    })),
  );
});

describe("Fold journal fault recovery and ownership", () => {
  const streamId = "fold/sessions/recovery/events";
  const refs = sessionRefs(streamId);
  const withContext = <E>(
    program: (store: StreamsyStore) => Effect.Effect<void, E, import("effect").Scope.Scope>,
    layer: Layer.Layer<StreamsReader | StreamsWriter> = Streams.layerMemory(),
  ) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layer);
          yield* program({ context, close: () => Promise.resolve() });
        }),
      ),
    );

  for (const when of ["before", "after"] as const) {
    test(`ambiguous log completion ${when} commit replays the exact tuple and bytes`, async () => {
      // create log + journal, Epoch, Pending, then the first producer append.
      const outcomes: string[] = [];
      const payloads: Uint8Array[] = [];
      const source = Protocol.layer().pipe(
        Layer.provide(faultyStorage(Memory.layer(), { failOn: 5, when })),
      );
      const observed = Layer.effectContext(
        Effect.gen(function* () {
          const reader = yield* StreamsReader;
          const writer = yield* StreamsWriter;
          return Context.make(StreamsReader, reader).pipe(
            Context.add(
              StreamsWriter,
              StreamsWriter.of({
                ...writer,
                append: (id, options) =>
                  Effect.gen(function* () {
                    if (id === refs.log.id) payloads.push(options.data.slice());
                    const result = yield* writer.append(id, options);
                    if (id === refs.log.id) outcomes.push(result.status);
                    return result;
                  }),
              }),
            ),
          );
        }),
      ).pipe(Layer.provide(source));
      await withContext(
        (store) =>
          Effect.gen(function* () {
            const log = yield* openLog(store, streamId, "create");
            const first = yield* log.append(sessionStarted());
            yield* log.append(sessionTitle("after retry"));
            const journal = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
            const pending = journal.items.filter((row) => row._tag === "Pending");
            expect(pending.map((row) => [row.entrySeq, row.producerSeq])).toEqual([
              [0, 0],
              [1, 1],
            ]);
            expect(pending[0]?.entryId).toBe(first.eventId);
            expect(yield* readFoldLog(store, streamId)).toHaveLength(2);
            expect(payloads[0]).toEqual(payloads[1]);
            expect(outcomes[0]).toBe(when === "after" ? "duplicate" : "appended");
          }),
        observed,
      );
    });
  }

  test("crash between Pending and log resumes the exact journaled payload before any new entry", async () => {
    await withContext((store) =>
      Effect.gen(function* () {
        const log = yield* openLog(store, streamId, "create");
        yield* log.append(sessionStarted());
        const pending = yield* plantPending(store);
        const resumed = yield* openLog(store, streamId, "resume");
        yield* resumed.append(sessionTitle("new payload"));
        const history = yield* readHistory(refs.log).pipe(Effect.provide(store.context));
        expect(history.items[1]).toEqual(pending.entry);
        expect(history.items).toHaveLength(3);
        const journal = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
        expect(journal.items.map((row) => row._tag)).toEqual([
          "Epoch",
          "Pending",
          "Pending",
          "Epoch",
          "Pending",
        ]);
        expect(journal.items.at(-1)).toMatchObject({ entrySeq: 2, producerSeq: 2, epoch: 0 });
      }),
    );
  });

  test("takeover settles old Pending before its Epoch and fences both old ownership and producer tuple", async () => {
    await withContext((store) =>
      Effect.gen(function* () {
        const old = yield* openLog(store, streamId, "create");
        yield* old.append(sessionStarted());
        const pending = yield* plantPending(store);
        const owner = yield* openLog(store, streamId, "takeover");
        yield* owner.append(sessionTitle("new epoch"));
        const failure = yield* Effect.flip(old.append(sessionTitle("stale owner")));
        expect(failure.message).toContain("Fenced: stale-epoch");
        const result = yield* Producer.append(refs.log, [pending.entry], {
          producerId: refs.producerId,
          epoch: 0,
          seq: 1,
        }).pipe(Effect.provide(store.context));
        expect(result.status).toBe("stale-epoch");
        const journal = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
        expect(journal.items.map((row) => row._tag)).toEqual([
          "Epoch",
          "Pending",
          "Pending",
          "Epoch",
          "Pending",
        ]);
        expect(journal.items[3]).toEqual({ _tag: "Epoch", epoch: 1, reason: "takeover" });
        expect(journal.items[4]).toMatchObject({ epoch: 1, producerSeq: 0, entrySeq: 2 });
        const history = yield* readHistory(refs.log).pipe(Effect.provide(store.context));
        expect(history.items[1]).toEqual(pending.entry);
        expect(history.items).toHaveLength(3);
      }),
    );
  });

  test("resume with an explicit mismatched epoch changes neither journal nor log", async () => {
    await withContext((store) =>
      Effect.gen(function* () {
        const log = yield* openLog(store, streamId, "create");
        yield* log.append(sessionStarted());
        const before = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
        const failure = yield* Effect.flip(
          makeEventLog({ store, streamId, mode: "resume", epoch: 1 }),
        );
        expect(failure.message).toContain("requested epoch");
        expect(yield* readHistory(refs.journal).pipe(Effect.provide(store.context))).toEqual(
          before,
        );
      }),
    );
  });

  test("payload disagreement in acknowledged journal history is corruption", async () => {
    await withContext((store) =>
      Effect.gen(function* () {
        const log = yield* openLog(store, streamId, "create");
        yield* log.append(sessionStarted());
        yield* plantPending(store);
        yield* Streams.append(refs.log, [{ ...rawTitleEntry(1), title: "different bytes" }]).pipe(
          Effect.provide(store.context),
        );
        const failure = yield* Effect.flip(openLog(store, streamId, "resume"));
        expect(failure._tag).toBe("EventLogCorruptEntryError");
        expect(failure.message).toContain("payload differs");
      }),
    );
  });

  test("five exhausted attempts leave Pending intact and prohibit a replacement payload", async () => {
    await withContext((store) =>
      Effect.gen(function* () {
        const writer = Context.get(store.context, StreamsWriter);
        let calls = 0;
        const failingStore: StreamsyStore = {
          ...store,
          context: Context.add(
            store.context,
            StreamsWriter,
            StreamsWriter.of({
              ...writer,
              append: (id, options) =>
                id === refs.log.id
                  ? Effect.suspend(() => {
                      calls++;
                      return Effect.fail(
                        new StorageFault({
                          operation: "mutate",
                          message: "outage",
                          retryable: true,
                        }),
                      );
                    })
                  : writer.append(id, options),
            }),
          ),
        };
        const log = yield* openLog(failingStore, streamId, "create");
        expect(yield* Effect.flip(log.append(sessionStarted()))).toMatchObject({ retryable: true });
        expect(calls).toBe(5);
        const before = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
        expect((yield* Effect.flip(log.append(sessionStarted()))).message).toContain(
          "resume from the journal",
        );
        expect(yield* readHistory(refs.journal).pipe(Effect.provide(store.context))).toEqual(
          before,
        );
        const recovered = yield* openLog(store, streamId, "resume");
        yield* recovered.append(sessionTitle("after recovery"));
        expect(yield* readFoldLog(store, streamId)).toHaveLength(2);
      }),
    );
  });

  test("ambiguous journal commit is recovered without minting a second Pending", async () => {
    await withContext(
      (store) =>
        Effect.gen(function* () {
          const log = yield* openLog(store, streamId, "create");
          const failed = yield* Effect.flip(log.append(sessionStarted()));
          expect(failed).toMatchObject({ retryable: true });
          expect((yield* Effect.flip(log.append(sessionStarted()))).message).toContain(
            "resume from the journal",
          );
          const resumed = yield* openLog(store, streamId, "resume");
          yield* resumed.append(sessionTitle("recovered"));
          const journal = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
          expect(
            journal.items.filter((row) => row._tag === "Pending").map((row) => row.entrySeq),
          ).toEqual([0, 1]);
          expect(yield* readFoldLog(store, streamId)).toHaveLength(2);
        }),
      Protocol.layer().pipe(
        Layer.provide(faultyStorage(Memory.layer(), { failOn: 4, when: "after" })),
      ),
    );
  });

  test("takeover while the old owner is parked after journaling settles before epoch advance", async () => {
    await withContext((store) =>
      Effect.gen(function* () {
        const parked = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const writer = Context.get(store.context, StreamsWriter);
        let hold = false;
        const oldStore: StreamsyStore = {
          ...store,
          context: Context.add(
            store.context,
            StreamsWriter,
            StreamsWriter.of({
              ...writer,
              append: (id, options) =>
                Effect.gen(function* () {
                  if (hold && id === refs.log.id) {
                    yield* Deferred.succeed(parked, undefined);
                    yield* Deferred.await(release);
                  }
                  return yield* writer.append(id, options);
                }),
            }),
          ),
        };
        const old = yield* openLog(oldStore, streamId, "create");
        yield* old.append(sessionStarted());
        hold = true;
        const running = yield* Effect.forkChild(
          Effect.exit(old.append(sessionTitle("parked payload"))),
        );
        yield* Deferred.await(parked);
        const before = yield* readHistory(refs.journal).pipe(Effect.provide(store.context));
        const pending = before.items.at(-1);
        if (pending?._tag !== "Pending") throw new Error("expected durable Pending");
        const takeoverStore: StreamsyStore = {
          ...store,
          context: Context.add(
            store.context,
            StreamsWriter,
            StreamsWriter.of({
              ...writer,
              append: (id, options) =>
                Effect.gen(function* () {
                  if (id === refs.journal.id) {
                    // The first takeover write is Epoch: old payload must already be in log.
                    const history = yield* readHistory(refs.log).pipe(
                      Effect.provide(store.context),
                      Effect.orDie,
                    );
                    expect(history.items[1]).toEqual(pending.entry);
                  }
                  return yield* writer.append(id, options);
                }),
            }),
          ),
        };
        const owner = yield* openLog(takeoverStore, streamId, "takeover");
        yield* owner.append(sessionTitle("new epoch"));
        yield* Deferred.succeed(release, undefined);
        const exit = yield* Fiber.join(running);
        expect(failureOf(exit).message).toContain("stale-epoch");
        const history = yield* readFoldLog(store, streamId);
        expect(history.map((entry) => entry.seq)).toEqual([0, 1, 2]);
        expect(String(history[1]?.eventId)).toBe(pending.entryId);
      }),
    );
  });

  for (const operation of ["read", "create"] as const) {
    test(`a scripted ${operation} capability failure remains typed and retryable`, async () => {
      await withContext((store) =>
        Effect.gen(function* () {
          const fault = new StorageFault({
            operation,
            message: "scripted outage",
            retryable: true,
          });
          const context =
            operation === "read"
              ? Context.add(
                  store.context,
                  StreamsReader,
                  StreamsReader.of({
                    ...Context.get(store.context, StreamsReader),
                    read: () => Effect.fail(fault),
                  }),
                )
              : Context.add(
                  store.context,
                  StreamsWriter,
                  StreamsWriter.of({
                    ...Context.get(store.context, StreamsWriter),
                    create: () => Effect.fail(fault),
                  }),
                );
          const failure = yield* Effect.flip(
            openLog({ ...store, context }, streamId, operation === "read" ? "resume" : "create"),
          );
          expect(failure).toMatchObject({
            _tag: "EventLogUnavailableError",
            message: "scripted outage",
            retryable: true,
          });
        }),
      );
    });
  }

  const plantPending = (store: StreamsyStore) =>
    Effect.gen(function* () {
      const entry = rawTitleEntry(1);
      const decoded = yield* decodeStoredLogEntry(entry);
      const pending: Pending = {
        _tag: "Pending",
        epoch: 0,
        producerSeq: 1,
        entrySeq: 1,
        entryId: decoded.eventId,
        entry,
      };
      const journal = yield* readHistory(refs.journal);
      yield* appendJournal(refs, pending, journal.offset);
      return pending;
    }).pipe(Effect.provide(store.context));
});
