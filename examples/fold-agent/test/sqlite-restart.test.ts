/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow: every test callback is a Promise the runner awaits, and the CLI under test is spawned as a real child process with temp-dir plumbing. */
/**
 * The CLI seam. `inspect` is the interesting one: reading durable agent state
 * must work in a separate process with no provider credentials at all.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { Database } from "bun:sqlite";
import { SessionId, startSession } from "@humanlayer/fold-core";
import { Producer, Streams, StreamsReader, StreamRef } from "@streamsy/core";
import { Context, Effect, Schema } from "effect";
import { exampleAgent } from "../src/agent.ts";
import { readHistory, sessionRefs } from "../src/session-journal.ts";
import { openStore, sessionStreamId } from "../src/storage.ts";
import { makeEventLog, readFoldLog, streamsyEventLog } from "../src/streamsy-event-log.ts";
import { scriptedModel, textTurn, toolCallTurn } from "./fixtures/scripted-model.ts";

const cli = join(dirname(import.meta.dir), "src", "cli.ts");
const worker = join(import.meta.dir, "sqlite-process-worker.ts");

const runCli = async (args: ReadonlyArray<string>, env: Record<string, string>) => {
  const proc = Bun.spawn(["bun", "run", cli, ...args], {
    env: { ...process.env, ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
};

const runWorker = async (args: ReadonlyArray<string>) => {
  const proc = Bun.spawn(["bun", "run", worker, ...args], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
};

const withTempDir = <A>(run: (dir: string) => Promise<A>): Promise<A> => {
  const dir = mkdtempSync(join(tmpdir(), "fold-streamsy-cli-"));
  return run(dir).finally(() => rmSync(dir, { recursive: true, force: true }));
};

/** Step 2 SQLite gate: seed a real durable session so `inspect` has something honest to read. */
const seedSession = async (filename: string, streamId: string, sessionId: SessionId) => {
  const store = await Effect.runPromise(openStore({ filename }));
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const scripted = yield* scriptedModel([
            toolCallTurn("provider-call-1", "text_stats", { text: "hello" }),
            textTurn("Five characters."),
          ]);
          const session = yield* startSession({
            agent: exampleAgent(scripted.model),
            log: streamsyEventLog({ store, streamId, mode: "create" }),
            sessionId,
          });
          yield* session.send("How long is 'hello'?");
        }),
      ),
    );
  } finally {
    await store.close();
  }
};

describe("fold-agent CLI", () => {
  test("inspects a seeded SQLite session without provider credentials", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const sessionId = SessionId.create();
      const streamId = sessionStreamId(sessionId);
      await seedSession(filename, streamId, sessionId);

      const result = await runCli(["inspect", streamId], {
        FOLD_AGENT_DB: filename,
        OPENAI_API_KEY: "",
        ANTHROPIC_API_KEY: "",
      });

      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("session_started");
      expect(result.stdout).toContain("text_stats");
      expect(result.stdout).toContain("agent-finished");
      expect(result.stdout).toContain(`stream id: ${streamId}`);
    });
  }, 60_000);

  test("runs start, resume and inspect as separate CLI processes over FOLD_AGENT_DB", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const common = {
        FOLD_AGENT_DB: filename,
        OPENAI_API_KEY: "",
        ANTHROPIC_API_KEY: "",
      };
      const first = await runCli(["start", "How long is hello?"], {
        ...common,
        FOLD_AGENT_SCRIPT: JSON.stringify([
          { type: "tool", id: "cli-call-1", name: "text_stats", params: { text: "hello" } },
          { type: "text", text: "Five characters." },
        ]),
      });
      expect(first.exitCode).toBe(0);
      const streamId = /^stream id:\s+(.+)$/m.exec(first.stdout)?.[1];
      expect(streamId).toStartWith("fold/sessions/");
      if (streamId === undefined) throw new Error(`missing stream id: ${first.stdout}`);

      const second = await runCli(["resume", streamId, "What happened?"], {
        ...common,
        FOLD_AGENT_SCRIPT: JSON.stringify([{ type: "text", text: "The tool counted hello." }]),
      });
      expect(second.exitCode).toBe(0);
      expect(second.stdout).toContain("The tool counted hello.");

      const inspected = await runCli(["inspect", streamId], {
        ...common,
        FOLD_AGENT_SCRIPT: "",
      });
      expect(inspected.exitCode).toBe(0);
      expect(inspected.stdout).toContain("text_stats");
      expect(inspected.stdout).toContain("The tool counted hello.");
    });
  }, 60_000);
});

describe("Fold retained-file process recovery", () => {
  test("reopens the same producer identity and advances it in a later writer process", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const streamId = sessionStreamId(SessionId.create());
      expect((await runWorker(["create", filename, streamId])).exitCode).toBe(0);
      expect((await runWorker(["append", filename, streamId, "later process"])).exitCode).toBe(0);
      const store = await Effect.runPromise(openStore({ filename }));
      try {
        const refs = sessionRefs(streamId);
        const journal = await Effect.runPromise(
          readHistory(refs.journal).pipe(Effect.provide(store.context)),
        );
        expect(journal.items.filter((row) => row._tag === "Pending")).toMatchObject([
          { epoch: 0, producerSeq: 0, entrySeq: 0 },
          { epoch: 0, producerSeq: 1, entrySeq: 1 },
        ]);
        const acknowledged = journal.items.at(-1);
        if (acknowledged?._tag !== "Pending") throw new Error("expected acknowledged Pending");
        expect(
          await Effect.runPromise(
            Producer.append(refs.log, [acknowledged.entry], {
              producerId: refs.producerId,
              epoch: acknowledged.epoch,
              seq: acknowledged.producerSeq,
            }).pipe(Effect.provide(store.context)),
          ),
        ).toMatchObject({ _tag: "Duplicate" });
        expect(
          (await Effect.runPromise(readFoldLog(store, streamId))).map((row) => row.seq),
        ).toEqual([0, 1]);
      } finally {
        await store.close();
      }
    });
  }, 60_000);

  test("recovers exact journal bytes after a process exits immediately after log append", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const crashBytesPath = join(dir, "crash-append.bin");
      const recoveryBytesPath = join(dir, "recovery-append.bin");
      const streamId = sessionStreamId(SessionId.create());
      expect((await runWorker(["create", filename, streamId])).exitCode).toBe(0);
      const crashed = await runWorker([
        "crash-after-log-append",
        filename,
        streamId,
        "ambiguous bytes",
        crashBytesPath,
      ]);
      expect(crashed.exitCode).toBe(86);

      const refs = sessionRefs(streamId);
      const crashBytes = new Uint8Array(readFileSync(crashBytesPath));
      const database = new Database(filename, { readonly: true });
      try {
        const persisted = database
          .query<{ data: Uint8Array }, [string]>(
            "SELECT data FROM streamsy_messages WHERE stream_id=? ORDER BY offset DESC LIMIT 1",
          )
          .get(refs.log.id);
        expect(persisted).not.toBeNull();
        // application/json appends carry a JSON array; the protocol framer stores each
        // element byte-for-byte as its own message BLOB.
        expect(new Uint8Array(persisted?.data ?? [])).toEqual(crashBytes.slice(1, -1));
      } finally {
        database.close(false);
      }

      const between = await Effect.runPromise(openStore({ filename }));
      let pendingEntry: Schema.Json;
      try {
        const journal = await Effect.runPromise(
          readHistory(refs.journal).pipe(Effect.provide(between.context)),
        );
        const pending = journal.items.at(-1);
        if (pending?._tag !== "Pending") throw new Error("expected crash-retained Pending");
        pendingEntry = pending.entry;
        const logged = await Effect.runPromise(
          readHistory(refs.log).pipe(Effect.provide(between.context)),
        );
        expect(logged.items.at(-1)).toEqual(pending.entry);

        const unrelated = StreamRef.json("unrelated/activity", { schema: Schema.Json });
        expect(
          await Effect.runPromise(Streams.create(unrelated).pipe(Effect.provide(between.context))),
        ).toMatchObject({ _tag: "Created" });
        expect(
          await Effect.runPromise(
            Streams.append(unrelated, [{ happened: "between attempts" }]).pipe(
              Effect.provide(between.context),
            ),
          ),
        ).toMatchObject({ _tag: "Appended" });
        const activeSession = sessionStreamId(SessionId.create());
        expect((await runWorker(["create", filename, activeSession])).exitCode).toBe(0);
      } finally {
        await between.close();
      }

      const settled = await runWorker([
        "settle-pending",
        filename,
        streamId,
        "unused",
        recoveryBytesPath,
      ]);
      expect(settled.exitCode).toBe(0);
      expect(JSON.parse(settled.stdout)).toMatchObject({
        status: "settled",
        settlement: { _tag: "Duplicate", producerEpoch: 0, producerSeq: 1 },
      });
      expect(new Uint8Array(readFileSync(recoveryBytesPath))).toEqual(crashBytes);

      expect((await runWorker(["append", filename, streamId, "new input"])).exitCode).toBe(0);
      const recovered = await Effect.runPromise(openStore({ filename }));
      try {
        const history = await Effect.runPromise(
          readHistory(refs.log).pipe(Effect.provide(recovered.context)),
        );
        expect(history.items[1]).toEqual(pendingEntry);
        expect(history.items).toHaveLength(3);
        const journal = await Effect.runPromise(
          readHistory(refs.journal).pipe(Effect.provide(recovered.context)),
        );
        expect(journal.items.filter((row) => row._tag === "Pending")).toMatchObject([
          { epoch: 0, producerSeq: 0, entrySeq: 0 },
          { epoch: 0, producerSeq: 1, entrySeq: 1 },
          { epoch: 0, producerSeq: 2, entrySeq: 2 },
        ]);
      } finally {
        await recovered.close();
      }
    });
  }, 60_000);

  test("takeover in another process fences the stale retained owner", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const streamId = sessionStreamId(SessionId.create());
      expect((await runWorker(["create", filename, streamId])).exitCode).toBe(0);
      const staleStore = await Effect.runPromise(openStore({ filename }));
      try {
        const stale = await Effect.runPromise(
          Effect.scoped(makeEventLog({ store: staleStore, streamId, mode: "resume" })),
        );
        expect(
          (
            await runWorker([
              "crash-after-log-append",
              filename,
              streamId,
              "settle before takeover",
            ])
          ).exitCode,
        ).toBe(86);
        const refs = sessionRefs(streamId);
        const before = await Effect.runPromise(
          readHistory(refs.journal).pipe(Effect.provide(staleStore.context)),
        );
        const pending = before.items.at(-1);
        if (pending?._tag !== "Pending") throw new Error("expected takeover Pending");
        expect((await runWorker(["takeover", filename, streamId, "new owner"])).exitCode).toBe(0);
        const failure = await Effect.runPromise(
          Effect.flip(
            stale.append({
              _tag: "session_title",
              agentId: null,
              parentAgentId: null,
              toolCallId: null,
              title: "stale owner",
            }),
          ),
        );
        expect(failure.message).toContain("Fenced");
        const history = await Effect.runPromise(
          readHistory(refs.log).pipe(Effect.provide(staleStore.context)),
        );
        expect(history.items[1]).toEqual(pending.entry);
        const journal = await Effect.runPromise(
          readHistory(refs.journal).pipe(Effect.provide(staleStore.context)),
        );
        const takeoverIndex = journal.items.findIndex(
          (row) => row._tag === "Epoch" && row.reason === "takeover",
        );
        expect(takeoverIndex).toBeGreaterThan(journal.items.indexOf(pending));
      } finally {
        await staleStore.close();
      }
    });
  }, 60_000);

  test("a commit between separate log and journal reads settles safely before new input", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const streamId = sessionStreamId(SessionId.create());
      expect((await runWorker(["create", filename, streamId])).exitCode).toBe(0);
      const store = await Effect.runPromise(openStore({ filename }));
      try {
        const refs = sessionRefs(streamId);
        const reader = Context.get(store.context, StreamsReader);
        let raced = false;
        const racingStore = {
          ...store,
          context: Context.add(
            store.context,
            StreamsReader,
            StreamsReader.of({
              ...reader,
              read: (id, options) => {
                const batches = reader.read(id, options);
                if (id !== refs.log.id || raced) return batches;
                raced = true;
                return batches.pipe(
                  Effect.tap(() =>
                    Effect.promise(async () => {
                      const result = await runWorker([
                        "append",
                        filename,
                        streamId,
                        "raced append",
                      ]);
                      if (result.exitCode !== 0) throw new Error(result.stderr);
                    }),
                  ),
                );
              },
            }),
          ),
        };
        const resumed = await Effect.runPromise(
          Effect.scoped(makeEventLog({ store: racingStore, streamId, mode: "resume" })),
        );
        await Effect.runPromise(
          resumed.append({
            _tag: "session_title",
            agentId: null,
            parentAgentId: null,
            toolCallId: null,
            title: "after race",
          }),
        );
        expect(
          (await Effect.runPromise(readFoldLog(store, streamId))).map((row) => row.seq),
        ).toEqual([0, 1, 2]);
      } finally {
        await store.close();
      }
    });
  }, 60_000);
});
