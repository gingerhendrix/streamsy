/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow: every test callback is a Promise the runner awaits, and the CLI under test is spawned as a real child process with temp-dir plumbing. */
/**
 * The CLI seam. `inspect` is the interesting one: reading durable agent state
 * must work in a separate process with no provider credentials at all.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { SessionId, startSession } from "@humanlayer/fold-core";
import { Effect } from "effect";
import { exampleAgent } from "../src/agent.ts";
import { openStore, sessionStreamId } from "../src/storage.ts";
import { makeEventLog, readFoldLog, streamsyEventLog } from "../src/streamsy-event-log.ts";
import { scriptedModel, textTurn, toolCallTurn } from "./fixtures/scripted-model.ts";

const cli = join(dirname(import.meta.dir), "src", "cli.ts");
const worker = join(import.meta.dir, "support/sqlite-process-worker.ts");

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
  test("a second process that resumes the same session fences the first on its next append", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const streamId = sessionStreamId(SessionId.create());
      expect((await runWorker(["create", filename, streamId])).exitCode).toBe(0);
      const store = await Effect.runPromise(openStore({ filename }));
      try {
        const first = await Effect.runPromise(
          Effect.scoped(makeEventLog({ store, streamId, mode: "resume" })),
        );
        const second = await runWorker(["append", filename, streamId, "second process"]);
        expect(second.exitCode).toBe(0);
        const failure = await Effect.runPromise(
          Effect.flip(
            first.append({
              _tag: "session_title",
              agentId: null,
              parentAgentId: null,
              toolCallId: null,
              title: "first process is stale",
            }),
          ),
        );
        expect(failure.message).toContain("Fenced: another writer advanced the log");
        expect(
          (await Effect.runPromise(readFoldLog(store, streamId))).map((row) => row.seq),
        ).toEqual([0, 1]);
      } finally {
        await store.close();
      }
    });
  }, 60_000);
});
