/** Store acquisition is an example edge. One Scope owns one memory Layer. */
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core-next";
import { Context, Effect, Exit, Layer, Schema, Scope } from "effect";

export interface StreamsyStore {
  readonly context: Context.Context<StreamsReader | StreamsWriter>;
  readonly close: () => Promise<void>;
}
export interface StreamsyStoreOptions {
  readonly filename?: string;
  readonly longPollTimeoutMs?: number;
}
export class StorageNotAvailable extends Schema.TaggedError<StorageNotAvailable>()(
  "StorageNotAvailable",
  { message: Schema.String },
) {}
export const DEFAULT_DATABASE_PATH = "examples/fold-agent/.data/agent.sqlite";
export const databasePathFromEnv = (env: Record<string, string | undefined>): string =>
  env["FOLD_AGENT_DB"] ?? DEFAULT_DATABASE_PATH;
export const sessionStreamId = (sessionId: string): string => `fold/sessions/${sessionId}/events`;

const closeScope = (scope: Scope.Closeable) => () =>
  Effect.runPromise(Scope.close(scope, Exit.void));

export const openMemoryStore = (options: { longPollTimeoutMs?: number } = {}) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(Streams.layerMemory(options), scope).pipe(
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
    );
    return {
      context,
      close: closeScope(scope),
    } satisfies StreamsyStore;
  });

export const openStore = (options: StreamsyStoreOptions = {}) =>
  options.filename === undefined || options.filename === ":memory:"
    ? openMemoryStore(options)
    : Effect.fail(
        new StorageNotAvailable({
          message:
            "SQLite storage arrives with the next release step (Step 2); file-backed Fold storage is unavailable.",
        }),
      );
