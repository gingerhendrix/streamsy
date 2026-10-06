import { Layer } from "effect";
import { it as baseIt } from "@streamsy/effect-test";
import { describe, expect } from "bun:test";
import { BunServices } from "@effect/platform-bun";
import { Cause, ConfigProvider, Console, Effect, Exit, Option } from "effect";
import { Command } from "effect/cli";
import { cli } from "../src/cli.ts";
import { DatabasePath, DEFAULT_DATABASE_PATH } from "../src/storage.ts";
import { SessionId } from "@humanlayer/fold-core";

const databasePathFrom = (env: Record<string, string>) =>
  DatabasePath.pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
  );

/** Run the CLI in process with no provider settings, and keep what it prints. */
const runCommand = (args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const stdout: Array<string> = [];
    const stderr: Array<string> = [];

    const console: Console.Console = {
      ...globalThis.console,
      log: (...parts: ReadonlyArray<unknown>) => stdout.push(parts.join(" ")),
      error: (...parts: ReadonlyArray<unknown>) => stderr.push(parts.join(" ")),
    };

    const exit = yield* Command.runWith(cli, { version: "test" })(args).pipe(
      withLayer(BunServices.layer),
      Effect.provideService(Console.Console, console),
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env: {} })),
      Effect.exit,
    );

    const failure = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();

    return { exit, failure, stdout: stdout.join("\n"), stderr: stderr.join("\n") };
  });

describe("Fold CLI parsing and rendering", () => {
  baseIt.live("resume accepts a session stream id and prompt, then needs a model", () =>
    Effect.gen(function* () {
      const session = SessionId.create();
      const result = yield* runCommand(["resume", `fold/sessions/${session}/events`, "hello"]);
      expect(Option.map(result.failure, (error) => error._tag)).toEqual(Option.some("UserError"));
      expect(result.stderr).toContain("No provider credentials found");
    }),
  );
  baseIt.live("resume rejects missing arguments, flags, and other stream ids with help", () =>
    Effect.gen(function* () {
      const session = SessionId.create();

      for (const args of [
        ["resume"],
        ["resume", `fold/sessions/${session}/events`],
        ["resume", `fold/sessions/${session}/events`, "--other", "hi"],
        ["resume", "a", "hi"],
        ["resume", "fold/sessions/a/events", "hi"],
      ]) {
        const result = yield* runCommand(args);
        expect(Option.map(result.failure, (error) => error._tag)).toEqual(Option.some("ShowHelp"));
        expect(result.stdout).toContain("fold-agent resume [flags] <stream-id> <prompt...>");
        expect(result.stderr).toContain("ERROR");
      }
    }),
  );
  baseIt.live("start rejects an empty prompt, and an unknown command shows the command list", () =>
    Effect.gen(function* () {
      const empty = yield* runCommand(["start", " "]);
      expect(empty.stderr).toContain("Expected: a non-empty prompt");
      const unknown = yield* runCommand(["bogus"]);
      expect(unknown.stdout).toContain("SUBCOMMANDS");
      expect(unknown.stderr).toContain('Unknown subcommand "bogus"');
    }),
  );
  baseIt.live("reads the database path from FOLD_AGENT_DB, or uses the default", () =>
    Effect.gen(function* () {
      expect(yield* databasePathFrom({})).toBe(DEFAULT_DATABASE_PATH);
      expect(yield* databasePathFrom({ FOLD_AGENT_DB: "custom.sqlite" })).toBe("custom.sqlite");
    }),
  );
});

/** Build local fixtures in a scope and supply their Context to the program. */
const withLayer =
  <ROut, LE, RIn>(fixture: Layer.Layer<ROut, LE, RIn>) =>
  <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(fixture);

        return yield* program.pipe(Effect.provideContext(services));
      }),
    );
