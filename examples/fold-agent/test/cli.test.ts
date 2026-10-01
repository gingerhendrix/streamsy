/* oxlint-disable effecttsgo/async-function -- Bun owns the test execution edge. */
import { describe, expect, test } from "bun:test";
import { BunServices } from "@effect/platform-bun";
import { Cause, ConfigProvider, Console, Effect, Exit, Option } from "effect";
import { Command } from "effect/unstable/cli";
import { cli } from "../src/cli.ts";
import { formatEntry } from "../src/render.ts";
import { DatabasePath, DEFAULT_DATABASE_PATH } from "../src/storage.ts";
import { decodeStoredLogEntry, AgentId, EventId, SessionId } from "@humanlayer/fold-core";

const databasePathFrom = (env: Record<string, string>) =>
  Effect.runPromise(
    DatabasePath.pipe(
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
    ),
  );

/** Run the CLI in process with no provider settings, and keep what it prints. */
const runCommand = async (args: ReadonlyArray<string>) => {
  const stdout: Array<string> = [];
  const stderr: Array<string> = [];
  const console: Console.Console = {
    ...globalThis.console,
    log: (...parts: ReadonlyArray<unknown>) => stdout.push(parts.join(" ")),
    error: (...parts: ReadonlyArray<unknown>) => stderr.push(parts.join(" ")),
  };
  const exit = await Effect.runPromiseExit(
    Command.runWith(cli, { version: "test" })(args).pipe(
      Effect.provide(BunServices.layer),
      Effect.provideService(Console.Console, console),
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env: {} })),
    ),
  );
  const failure = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();
  return { exit, failure, stdout: stdout.join("\n"), stderr: stderr.join("\n") };
};

describe("Fold CLI parsing and rendering", () => {
  test("resume accepts a session stream id and prompt, then needs a model", async () => {
    const session = SessionId.create();
    const result = await runCommand(["resume", `fold/sessions/${session}/events`, "hello"]);
    expect(Option.map(result.failure, (error) => error._tag)).toEqual(Option.some("UserError"));
    expect(result.stderr).toContain("No provider credentials found");
  });
  test("resume rejects missing arguments, flags, and other stream ids with help", async () => {
    const session = SessionId.create();
    for (const args of [
      ["resume"],
      ["resume", `fold/sessions/${session}/events`],
      ["resume", `fold/sessions/${session}/events`, "--other", "hi"],
      ["resume", "a", "hi"],
      ["resume", "fold/sessions/a/events", "hi"],
    ]) {
      const result = await runCommand(args);
      expect(Option.map(result.failure, (error) => error._tag)).toEqual(Option.some("ShowHelp"));
      expect(result.stdout).toContain("fold-agent resume [flags] <stream-id> <prompt...>");
      expect(result.stderr).toContain("ERROR");
    }
  });
  test("start rejects an empty prompt, and an unknown command shows the command list", async () => {
    const empty = await runCommand(["start", " "]);
    expect(empty.stderr).toContain("Expected: a non-empty prompt");
    const unknown = await runCommand(["bogus"]);
    expect(unknown.stdout).toContain("SUBCOMMANDS");
    expect(unknown.stderr).toContain('Unknown subcommand "bogus"');
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
