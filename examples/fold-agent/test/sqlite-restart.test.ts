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
import { streamsyEventLog } from "../src/streamsy-event-log.ts";
import { scriptedModel, textTurn, toolCallTurn } from "./fixtures/scripted-model.ts";

const cli = join(dirname(import.meta.dir), "src", "cli.ts");

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
  test.skip("Step 2: SQLite cross-process Fold restart and CLI persistence smoke", async () => {
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
});
