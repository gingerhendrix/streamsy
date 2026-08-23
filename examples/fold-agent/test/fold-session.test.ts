/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow: every test callback is a Promise the runner awaits, and temp-dir plumbing is host wiring. The sessions under test stay Effect values run with `Effect.runPromise`. */
/**
 * The end-to-end claim: a real Fold Core session runs unchanged while Streamsy
 * owns its durable log, and a brand-new runtime over the same SQLite file can
 * resume it and keep going.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resumeSession, SessionId, startSession, type LogEntry } from "@humanlayer/fold-core";
import { Effect } from "effect";
import { exampleAgent } from "../src/agent.ts";
import { openStore, sessionStreamId } from "../src/storage.ts";
import { readFoldLog, streamsyEventLog } from "../src/streamsy-event-log.ts";
import { scriptedModel, textTurn, toolCallTurn } from "./fixtures/scripted-model.ts";

const withTempDir = <A>(run: (dir: string) => Promise<A>): Promise<A> => {
  const dir = mkdtempSync(join(tmpdir(), "fold-streamsy-"));
  return run(dir).finally(() => rmSync(dir, { recursive: true, force: true }));
};

const tags = (entries: ReadonlyArray<LogEntry>) => entries.map((entry) => entry._tag);

describe("Fold session over a Streamsy durable log", () => {
  test("runs a tool turn, survives a process restart, and resumes with context", async () => {
    await withTempDir(async (dir) => {
      const filename = join(dir, "agent.sqlite");
      const sessionId = SessionId.create();
      const streamId = sessionStreamId(sessionId);

      // --- first "process": start a fresh session and run one tool turn ---
      const firstStore = openStore({ filename, longPollTimeoutMs: 200 });
      const firstRun = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const scripted = yield* scriptedModel([
              toolCallTurn("provider-call-1", "text_stats", { text: "hello from Streamsy" }),
              textTurn("The text has 19 characters."),
            ]);
            const session = yield* startSession({
              agent: exampleAgent(scripted.model),
              log: streamsyEventLog({ binding: firstStore.bind(streamId), mode: "create" }),
              sessionId,
              cwd: dir,
            });
            const finished = yield* session.send("How long is 'hello from Streamsy'?");
            const entries = yield* session.entries;

            return { finished, entries, remaining: yield* scripted.remainingTurns };
          }),
        ),
      );
      await firstStore.close();

      expect(firstRun.remaining).toBe(0);
      expect(firstRun.finished.outcome).toBe("completed");
      expect(tags(firstRun.entries).slice(0, 2)).toEqual(["session_started", "agent_started"]);
      expect(tags(firstRun.entries)).toContain("tool-result");

      const firstSeqs = firstRun.entries.map((entry) => entry.seq);
      expect(firstSeqs).toEqual(firstSeqs.map((_, index) => index));

      // --- second "process": a fresh store, runtime, and session over the same file ---
      const secondStore = openStore({ filename, longPollTimeoutMs: 200 });
      const secondRun = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const scripted = yield* scriptedModel([textTurn("Earlier the tool reported 19.")]);
            const session = yield* resumeSession({
              agent: exampleAgent(scripted.model),
              log: streamsyEventLog({ binding: secondStore.bind(streamId), mode: "resume" }),
            });
            const finished = yield* session.send("What did the tool return earlier?");

            return { finished, sessionId: session.sessionId, entries: yield* session.entries };
          }),
        ),
      );
      await secondStore.close();

      // Identity is adopted from the log, not minted again.
      expect(secondRun.sessionId).toBe(sessionId);
      expect(secondRun.entries.filter((entry) => entry._tag === "session_started")).toHaveLength(1);
      expect(secondRun.entries.length).toBeGreaterThan(firstRun.entries.length);

      // --- third "process": read the durable log with no runtime at all ---
      const readStore = openStore({ filename });
      const durable = await Effect.runPromise(readFoldLog(readStore.bind(streamId)));
      await readStore.close();

      expect(durable.map((entry) => entry.seq)).toEqual(durable.map((_, index) => index));
      expect(tags(durable)).toEqual(tags(secondRun.entries));
    });
  }, 30_000);
});
