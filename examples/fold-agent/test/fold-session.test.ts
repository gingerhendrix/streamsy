/* oxlint-disable effecttsgo/async-function -- Bun owns Promise test callbacks; Fold session work runs in scoped Effects. */
/**
 * The end-to-end claim: a real Fold Core session runs unchanged while Streamsy
 * owns its durable log, and a new session scope over the same memory Layer can
 * resume it and keep going.
 */
import { describe, expect, test } from "bun:test";
import { resumeSession, SessionId, startSession, type LogEntry } from "@humanlayer/fold-core";
import { readHistory, sessionRefs } from "../src/session-journal.ts";
import { Effect } from "effect";
import { exampleAgent } from "../src/agent.ts";
import { openMemoryStore, sessionStreamId } from "../src/storage.ts";
import { readFoldLog, streamsyEventLog } from "../src/streamsy-event-log.ts";
import { scriptedModel, textTurn, toolCallTurn } from "./fixtures/scripted-model.ts";

const tags = (entries: ReadonlyArray<LogEntry>) => entries.map((entry) => entry._tag);

describe("Fold session over a Streamsy durable log", () => {
  test("rebuilds a tool session in a new scope over the same memory Layer and resumes epoch 0", async () => {
    const firstStore = await Effect.runPromise(openMemoryStore());
    try {
      const sessionId = SessionId.create();
      const streamId = sessionStreamId(sessionId);

      // --- first session: start a fresh session and run one tool turn ---

      const firstRun = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const scripted = yield* scriptedModel([
              toolCallTurn("provider-call-1", "text_stats", { text: "hello from Streamsy" }),
              textTurn("The text has 19 characters."),
            ]);
            const session = yield* startSession({
              agent: exampleAgent(scripted.model),
              log: streamsyEventLog({ store: firstStore, streamId, mode: "create" }),
              sessionId,
              cwd: "/memory-proof",
            });
            const finished = yield* session.send("How long is 'hello from Streamsy'?");
            const entries = yield* session.entries;

            return { finished, entries, remaining: yield* scripted.remainingTurns };
          }),
        ),
      );

      expect(firstRun.remaining).toBe(0);
      expect(firstRun.finished.outcome).toBe("completed");
      expect(tags(firstRun.entries).slice(0, 2)).toEqual(["session_started", "agent_started"]);
      expect(tags(firstRun.entries)).toContain("tool-result");

      const firstSeqs = firstRun.entries.map((entry) => entry.seq);
      expect(firstSeqs).toEqual(firstSeqs.map((_, index) => index));

      // --- new session scope; the acquired store context remains alive ---
      const secondStore = firstStore;
      const secondRun = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const scripted = yield* scriptedModel([textTurn("Earlier the tool reported 19.")]);
            const session = yield* resumeSession({
              agent: exampleAgent(scripted.model),
              log: streamsyEventLog({ store: secondStore, streamId, mode: "resume" }),
            });
            const finished = yield* session.send("What did the tool return earlier?");

            return { finished, sessionId: session.sessionId, entries: yield* session.entries };
          }),
        ),
      );

      // Identity is adopted from the log, not minted again.
      expect(secondRun.sessionId).toBe(sessionId);
      expect(secondRun.entries.filter((entry) => entry._tag === "session_started")).toHaveLength(1);
      expect(secondRun.entries.length).toBeGreaterThan(firstRun.entries.length);

      // --- inspect the stored history without a Fold session ---
      const readStore = firstStore;
      const durable = await Effect.runPromise(readFoldLog(readStore, streamId));

      expect(durable.map((entry) => entry.seq)).toEqual(durable.map((_, index) => index));
      expect(tags(durable)).toEqual(tags(secondRun.entries));
      const journal = await Effect.runPromise(
        readHistory(sessionRefs(streamId).journal).pipe(Effect.provide(firstStore.context)),
      );
      const pending = journal.items.filter((row) => row._tag === "Pending");
      expect(pending[firstRun.entries.length]).toMatchObject({
        epoch: 0,
        producerSeq: firstRun.entries.length,
        entrySeq: firstRun.entries.length,
      });
      expect(pending.map((row) => row.entrySeq)).toEqual(durable.map((entry) => entry.seq));
      expect(journal.items.filter((row) => row._tag === "Epoch")).toEqual([
        { _tag: "Epoch", epoch: 0, reason: "start" },
        { _tag: "Epoch", epoch: 0, reason: "resume" },
      ]);
    } finally {
      await firstStore.close();
    }
  }, 30_000);
});
