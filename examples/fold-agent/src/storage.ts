import { Streamsy } from "@streamsy/core";
/* oxlint-disable effecttsgo/node-builtin-import -- This executable example prepares its configured SQLite parent directory at the Bun edge. */
/** Store acquisition is an example edge. One Scope owns the Sessions stream and its storage. */
import { Memory } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Exit, Layer, Schema, Scope } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Sessions } from "./session-log.ts";

export interface StreamsyStore {
  readonly context: Context.Context<Sessions>;
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
    const context = yield* Layer.buildWithScope(
      Memory.layer(options, Streamsy.group(Sessions)),
      scope,
    ).pipe(
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
    );
    const store: StreamsyStore = { context, close: closeScope(scope) };
    return store;
  });

const openSqliteStore = (filename: string, options: StreamsyStoreOptions) =>
  Effect.gen(function* () {
    yield* Effect.tryPromise({
      try: () => mkdir(dirname(filename), { recursive: true }),
      catch: (cause) =>
        new StorageNotAvailable({
          message: `Cannot prepare SQLite directory for ${filename}: ${String(cause)}`,
        }),
    });
    const scope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(
      BunStorage.layer(
        {
          client: { filename },
          ...(options.longPollTimeoutMs === undefined
            ? {}
            : { longPollTimeoutMs: options.longPollTimeoutMs }),
        },
        Streamsy.group(Sessions),
      ),
      scope,
    ).pipe(
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
    );
    const store: StreamsyStore = { context, close: closeScope(scope) };
    return store;
  });

export const openStore = (options: StreamsyStoreOptions = {}) => {
  const filename = options.filename;
  return filename === undefined || filename === ":memory:"
    ? openMemoryStore(options)
    : openSqliteStore(filename, options);
};
