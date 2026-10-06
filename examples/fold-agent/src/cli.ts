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
import { BunRuntime, BunServices } from "@effect/platform-bun";
import { resumeSession, SessionId, startSession, type FoldSession } from "@humanlayer/fold-core";
import { Console, Effect, Option } from "effect";
import { Argument, CliError, Command } from "effect/cli";
import packageJson from "../package.json" with { type: "json" };
import { configuredModel, exampleAgent } from "./agent.ts";
import { formatEntry } from "./render.ts";
import { Sessions, sessionStreamId } from "./session-log.ts";
import { configuredStoreLayer } from "./storage.ts";
import { readFoldLog, streamsyEventLog } from "./streamsy-event-log.ts";

/** A stream id names a session only when it matches the `Sessions` template. */
const streamId = Argument.String("stream-id").pipe(
  Argument.withDescription("A session stream id, fold/sessions/<session>/events"),
  Argument.filterMap(
    (id) => Option.map(Sessions.parse(id), ({ session }) => session),
    () => "fold/sessions/<session>/events",
  ),
);

/** The rest of the command line, joined with spaces. */
const promptArgument = Argument.String("prompt").pipe(
  Argument.withDescription("The user message for this turn"),
  Argument.atLeast(1),
  Argument.map((parts) => parts.join(" ").trim()),
  Argument.filter(
    (text) => text !== "",
    () => "a non-empty prompt",
  ),
);

/** A failure of the command itself prints its message once and exits with code 1. */
const asUserError = <A, E extends { readonly message: string }, R>(
  effect: Effect.Effect<A, E, R>,
) =>
  Effect.mapError(effect, (cause) => new CliError.UserError({ cause, userMessage: cause.message }));

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
// oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each CLI command is an entry point with one store lifetime.
const withStore = Effect.provide(configuredStoreLayer);

const start = Command.make("start", { prompt: promptArgument }, ({ prompt }) =>
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
  }).pipe(asUserError),
).pipe(Command.withDescription("Start a fresh session and run one turn"));

const resume = Command.make(
  "resume",
  { session: streamId, prompt: promptArgument },
  ({ session, prompt }) =>
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
    }).pipe(asUserError),
).pipe(Command.withDescription("Continue a session from its stored log and run one turn"));

const inspect = Command.make("inspect", { session: streamId }, ({ session }) =>
  withStore(
    Effect.gen(function* () {
      const entries = yield* readFoldLog(session);

      for (const entry of entries) yield* Console.log(formatEntry(entry));
      yield* Console.log("");
      yield* Console.log(`stream id: ${sessionStreamId(session)}`);
      yield* Console.log(`entries:   ${entries.length}`);
    }),
  ).pipe(asUserError),
).pipe(Command.withDescription("Print a session's log; needs no model credentials"));

export const cli = Command.make("fold-agent").pipe(
  Command.withDescription(
    [
      "A Fold Core agent whose session log lives in a Streamsy stream.",
      "",
      "Environment:",
      "  FOLD_AGENT_DB      SQLite path (default: examples/fold-agent/.data/agent.sqlite)",
      "  OPENAI_API_KEY     use an OpenAI-compatible provider (first choice)",
      "  ANTHROPIC_API_KEY  use Anthropic when no OpenAI key is set",
      "  FOLD_AGENT_MODEL   override the provider model id",
      "  FOLD_AGENT_SCRIPT  deterministic JSON turns for provider-free verification",
    ].join("\n"),
  ),
  Command.withSubcommands([start, resume, inspect]),
);

if (import.meta.main) {
  BunRuntime.runMain(
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- BunRuntime owns the application scope at this CLI entry point.
    Command.run(cli, { version: packageJson.version }).pipe(Effect.provide(BunServices.layer)),
  );
}
