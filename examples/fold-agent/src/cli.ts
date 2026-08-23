#!/usr/bin/env bun
/* oxlint-disable effecttsgo/global-console, effecttsgo/global-console-in-effect -- This CLI's contract is plain stdout/stderr text for a human and for the CLI tests; routing it through a Logger would change the observable output format. */
/**
 * A local CLI over a Fold Core agent whose durable log lives in a Streamsy
 * stream.
 *
 *   start  <prompt>              start a fresh session and run one turn
 *   resume <stream-id> <prompt>  continue that session in a brand-new process
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
import { Cause, Effect, Exit, Schema } from "effect";
import { exampleAgent, MissingCredentialsError, modelFromEnv } from "./agent.ts";
import { formatEntry } from "./render.ts";
import { databasePathFromEnv, openStore, sessionStreamId, type StreamsyStore } from "./storage.ts";
import { readFoldLog, streamsyEventLog } from "./streamsy-event-log.ts";

const USAGE = `Usage:
  bun run src/cli.ts start   "<prompt>"
  bun run src/cli.ts resume  <stream-id> "<prompt>"
  bun run src/cli.ts inspect <stream-id>

Environment:
  FOLD_AGENT_DB      SQLite path (default: examples/fold-agent/.data/agent.sqlite)
  OPENAI_API_KEY     use an OpenAI-compatible provider (first choice)
  ANTHROPIC_API_KEY  use Anthropic when no OpenAI key is set
  FOLD_AGENT_MODEL   override the provider model id`;

/** A command line that cannot be accepted. Printed as usage, exit code 1. */
class UsageError extends Schema.TaggedError<UsageError>()("UsageError", {
  message: Schema.String,
}) {}

const usage = () => new UsageError({ message: USAGE });

const printTurn = (session: FoldSession, streamId: string) =>
  Effect.gen(function* () {
    const entries = yield* session.entries;
    for (const entry of entries) console.log(formatEntry(entry));
    console.log("");
    console.log(`stream id:  ${streamId}`);
    console.log(`session id: ${session.sessionId}`);
    console.log(`entries:    ${entries.length}`);
  });

const withStore = <A, E>(run: (store: StreamsyStore) => Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const store = openStore({ filename: databasePathFromEnv(process.env) });
    return yield* run(store).pipe(Effect.ensuring(Effect.promise(() => store.close())));
  });

const start = (prompt: string) =>
  Effect.gen(function* () {
    const model = yield* modelFromEnv(process.env);
    const sessionId = SessionId.create();
    const streamId = sessionStreamId(sessionId);

    return yield* withStore((store) =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* startSession({
            agent: exampleAgent(model),
            log: streamsyEventLog({ binding: store.bind(streamId), mode: "create" }),
            sessionId,
            cwd: process.cwd(),
          });
          yield* session.send(prompt);
          yield* printTurn(session, streamId);
        }),
      ),
    );
  });

const resume = (streamId: string, prompt: string) =>
  Effect.gen(function* () {
    const model = yield* modelFromEnv(process.env);

    return yield* withStore((store) =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* resumeSession({
            agent: exampleAgent(model),
            log: streamsyEventLog({ binding: store.bind(streamId), mode: "resume" }),
          });
          yield* session.send(prompt);
          yield* printTurn(session, streamId);
        }),
      ),
    );
  });

const inspect = (streamId: string) =>
  withStore((store) =>
    readFoldLog(store.bind(streamId)).pipe(
      Effect.tap((entries) =>
        Effect.sync(() => {
          for (const entry of entries) console.log(formatEntry(entry));
          console.log("");
          console.log(`stream id: ${streamId}`);
          console.log(`entries:   ${entries.length}`);
        }),
      ),
    ),
  );

type CliError = UsageError | MissingCredentialsError | EventLogError | SubagentNotFoundError;

const main = (argv: ReadonlyArray<string>): Effect.Effect<unknown, CliError> => {
  const [command, ...rest] = argv;
  switch (command) {
    case "start": {
      const prompt = rest.join(" ").trim();
      if (prompt === "") return Effect.fail(usage());
      return start(prompt);
    }
    case "resume": {
      const [streamId, ...promptParts] = rest;
      const prompt = promptParts.join(" ").trim();
      if (streamId === undefined || prompt === "") return Effect.fail(usage());
      return resume(streamId, prompt);
    }
    case "inspect": {
      const [streamId] = rest;
      if (streamId === undefined) return Effect.fail(usage());
      return inspect(streamId);
    }
    default:
      return Effect.fail(usage());
  }
};

const exit = await Effect.runPromiseExit(main(process.argv.slice(2)).pipe(Effect.asVoid));
if (Exit.isFailure(exit)) {
  const failure = Cause.findErrorOption(exit.cause);
  console.error(
    failure._tag === "Some" && failure.value instanceof Error
      ? failure.value.message
      : Cause.pretty(exit.cause),
  );
  process.exitCode = 1;
}
