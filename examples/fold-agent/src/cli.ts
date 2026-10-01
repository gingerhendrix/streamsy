#!/usr/bin/env bun
/**
 * A local CLI over a Fold Core agent whose durable log lives in a Streamsy
 * stream.
 *
 *   start  <prompt>              start a fresh session and run one turn
 *   resume <stream-id> <prompt>  continue that session from SQLite
 *   inspect <stream-id>          print the durable log without touching a model
 *
 * `start` and `resume` need provider credentials; `inspect` deliberately does
 * not, because reading durable state should never require an API key.
 */
import {
  resumeSession,
  SessionId,
  startSession,
  type EventLogError,
  type FoldSession,
  type SubagentNotFoundError,
} from "@humanlayer/fold-core";
import { StorageFault } from "@streamsy/core";
import { Cause, Config, Console, Effect, Exit, Option, Schema } from "effect";
import { configuredModel, exampleAgent, MissingCredentialsError } from "./agent.ts";
import { formatEntry } from "./render.ts";
import { Sessions, sessionStreamId } from "./session-log.ts";
import { configuredStoreLayer, StorageNotAvailable } from "./storage.ts";
import { readFoldLog, streamsyEventLog } from "./streamsy-event-log.ts";

const USAGE = `Usage:
  bun run src/cli.ts start   "<prompt>"
  bun run src/cli.ts resume  <stream-id> "<prompt>"
  bun run src/cli.ts inspect <stream-id>

Environment:
  FOLD_AGENT_DB      SQLite path (default: examples/fold-agent/.data/agent.sqlite)
  OPENAI_API_KEY     use an OpenAI-compatible provider (first choice)
  ANTHROPIC_API_KEY  use Anthropic when no OpenAI key is set
  FOLD_AGENT_MODEL   override the provider model id
  FOLD_AGENT_SCRIPT  deterministic JSON turns for provider-free verification`;

/** A command line that cannot be accepted. Printed as usage, exit code 1. */
class UsageError extends Schema.TaggedError<UsageError>()("UsageError", {
  message: Schema.String,
}) {}

const usage = () => new UsageError({ message: USAGE });

/** A stream id names a session only when it matches the `Sessions` template. */
const sessionOf = (streamId: string) =>
  Option.match(Sessions.parse(streamId), {
    onNone: () =>
      Effect.fail(new UsageError({ message: `Not a Fold session stream: ${streamId}` })),
    onSome: ({ session }) => Effect.succeed(session),
  });

const printTurn = (fold: FoldSession, session: SessionId) =>
  Effect.gen(function* () {
    const entries = yield* fold.entries;
    for (const entry of entries) yield* Console.log(formatEntry(entry));
    yield* Console.log("");
    yield* Console.log(`stream id:  ${sessionStreamId(session)}`);
    yield* Console.log(`session id: ${fold.sessionId}`);
    yield* Console.log(`entries:    ${entries.length}`);
  });

/** Open the configured store for one command, and close it when the command ends. */
const withStore = Effect.provide(configuredStoreLayer);

const start = (prompt: string) =>
  Effect.gen(function* () {
    const model = yield* configuredModel;
    const session = SessionId.create();

    return yield* withStore(
      Effect.scoped(
        Effect.gen(function* () {
          const fold = yield* startSession({
            agent: exampleAgent(model),
            log: yield* streamsyEventLog({ session, mode: "create" }),
            sessionId: session,
            cwd: process.cwd(),
          });
          yield* fold.send(prompt);
          yield* printTurn(fold, session);
        }),
      ),
    );
  });

const resume = (session: SessionId, prompt: string) =>
  Effect.gen(function* () {
    const model = yield* configuredModel;

    return yield* withStore(
      Effect.scoped(
        Effect.gen(function* () {
          const fold = yield* resumeSession({
            agent: exampleAgent(model),
            log: yield* streamsyEventLog({ session, mode: "resume" }),
          });
          yield* fold.send(prompt);
          yield* printTurn(fold, session);
        }),
      ),
    );
  });

const inspect = (session: SessionId) =>
  withStore(
    Effect.gen(function* () {
      const entries = yield* readFoldLog(session);
      for (const entry of entries) yield* Console.log(formatEntry(entry));
      yield* Console.log("");
      yield* Console.log(`stream id: ${sessionStreamId(session)}`);
      yield* Console.log(`entries:   ${entries.length}`);
    }),
  );

export const parseResumeArgs = (args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const [streamId, ...rest] = args;
    const promptParts: string[] = [];
    for (const arg of rest) {
      if (arg.startsWith("--")) return yield* usage();
      promptParts.push(arg);
    }
    const prompt = promptParts.join(" ").trim();
    if (!streamId || !prompt) return yield* usage();
    return { session: yield* sessionOf(streamId), prompt };
  });

type CliError =
  | StorageNotAvailable
  | StorageFault
  | UsageError
  | MissingCredentialsError
  | Config.ConfigError
  | EventLogError
  | SubagentNotFoundError;

const main = (argv: ReadonlyArray<string>): Effect.Effect<unknown, CliError> => {
  const [command, ...rest] = argv;
  switch (command) {
    case "start": {
      const prompt = rest.join(" ").trim();
      if (prompt === "") return Effect.fail(usage());
      return start(prompt);
    }
    case "resume": {
      return parseResumeArgs(rest).pipe(
        Effect.flatMap(({ session, prompt }) => resume(session, prompt)),
      );
    }
    case "inspect": {
      const [streamId] = rest;
      if (streamId === undefined) return Effect.fail(usage());
      return sessionOf(streamId).pipe(Effect.flatMap(inspect));
    }
    default:
      return Effect.fail(usage());
  }
};

/** A typed failure prints its message. Anything else prints the full cause. */
const describe = (cause: Cause.Cause<CliError>): string => {
  const failure = Cause.findErrorOption(cause);
  return Option.isSome(failure) ? failure.value.message : Cause.pretty(cause);
};

if (import.meta.main) {
  const exit = await Effect.runPromiseExit(main(process.argv.slice(2)));
  if (Exit.isFailure(exit)) {
    await Effect.runPromise(Console.error(describe(exit.cause)));
    process.exitCode = 1;
  }
}
