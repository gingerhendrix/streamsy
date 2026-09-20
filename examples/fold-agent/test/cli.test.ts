/* oxlint-disable effecttsgo/async-function -- Bun owns the test execution edge. */
import { describe, expect, test } from "bun:test";
import { Effect, Exit } from "effect";
import { parseResumeArgs } from "../src/cli.ts";
import { formatEntry } from "../src/render.ts";
import { databasePathFromEnv, DEFAULT_DATABASE_PATH } from "../src/storage.ts";
import { decodeStoredLogEntry, AgentId, EventId, SessionId } from "@humanlayer/fold-core";

describe("Fold CLI parsing and rendering", () => {
  test("resume accepts a stream id and prompt", async () => {
    expect(await Effect.runPromise(parseResumeArgs(["fold/sessions/a/events", "hello"]))).toEqual({
      streamId: "fold/sessions/a/events",
      prompt: "hello",
    });
  });
  test("resume rejects missing arguments and flags", async () => {
    for (const args of [[], ["a"], ["a", "--other", "hi"]]) {
      expect(Exit.isFailure(await Effect.runPromiseExit(parseResumeArgs(args)))).toBe(true);
    }
  });
  test("inspect rendering needs no model or provider credentials", async () => {
    const entry = await Effect.runPromise(
      decodeStoredLogEntry({
        _tag: "session_started",
        seq: 0,
        eventId: EventId.create(),
        ts: 1,
        version: 1,
        agentId: null,
        parentAgentId: null,
        toolCallId: null,
        cwd: null,
        sessionId: SessionId.create(),
        rootAgentId: AgentId.create(),
        meta: {},
      }),
    );
    expect(formatEntry(entry)).toContain("session_started");
    if (entry._tag !== "session_started") throw new Error("expected session_started");
    expect(formatEntry(entry)).toContain(`session=${entry.sessionId}`);
  });
  test("retains database path configuration", () => {
    expect(databasePathFromEnv({})).toBe(DEFAULT_DATABASE_PATH);
    expect(databasePathFromEnv({ FOLD_AGENT_DB: "custom.sqlite" })).toBe("custom.sqlite");
  });
});
