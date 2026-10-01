/* oxlint-disable effecttsgo/async-function -- Bun owns the test execution edge. */
import { describe, expect, test } from "bun:test";
import { ConfigProvider, Effect, Exit } from "effect";
import { parseResumeArgs } from "../src/cli.ts";
import { formatEntry } from "../src/render.ts";
import { DatabasePath, DEFAULT_DATABASE_PATH } from "../src/storage.ts";
import { decodeStoredLogEntry, AgentId, EventId, SessionId } from "@humanlayer/fold-core";

const databasePathFrom = (env: Record<string, string>) =>
  Effect.runPromise(
    DatabasePath.pipe(
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
    ),
  );

describe("Fold CLI parsing and rendering", () => {
  test("resume accepts a session stream id and prompt", async () => {
    const session = SessionId.create();
    expect(
      await Effect.runPromise(parseResumeArgs([`fold/sessions/${session}/events`, "hello"])),
    ).toEqual({ session, prompt: "hello" });
  });
  test("resume rejects missing arguments, flags, and other stream ids", async () => {
    for (const args of [
      [],
      ["a"],
      ["a", "--other", "hi"],
      ["a", "hi"],
      ["fold/sessions/a/events", "hi"],
    ]) {
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
  test("reads the database path from FOLD_AGENT_DB, with a default beside the example", async () => {
    expect(await databasePathFrom({})).toBe(DEFAULT_DATABASE_PATH);
    expect(DEFAULT_DATABASE_PATH).toEndWith("examples/fold-agent/.data/agent.sqlite");
    expect(await databasePathFrom({ FOLD_AGENT_DB: "custom.sqlite" })).toBe("custom.sqlite");
  });
});
