/** The store is a Layer. The same `Sessions` service runs on memory or on Bun SQLite. */
import { MemoryStorage, Streamsy, type StorageFault } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { Config, Effect, Layer } from "effect";
import { Sessions } from "./session-log.ts";

export interface StoreOptions {
  readonly filename?: string;
  readonly longPollTimeoutMs?: number;
}

/** Next to the example, whatever the working directory is. */
export const DEFAULT_DATABASE_PATH = Bun.fileURLToPath(
  new URL("../.data/agent.sqlite", import.meta.url),
);

export const DatabasePath = Config.String("FOLD_AGENT_DB").pipe(
  Config.withDefault(DEFAULT_DATABASE_PATH),
);

const sessions = Streamsy.group(Sessions);

export const memoryLayer = (options: { readonly longPollTimeoutMs?: number } = {}) =>
  MemoryStorage.layer(sessions, options);

/** `BunStorage.layer` creates the parent folder of a new file. */
export const sqliteLayer = (filename: string, options: StoreOptions = {}) =>
  BunStorage.layer(sessions, {
    client: { filename },
    longPollTimeoutMs: options.longPollTimeoutMs,
  });

/** Memory for `:memory:` or no filename, Bun SQLite otherwise. */
export const storeLayer = (options: StoreOptions = {}): Layer.Layer<Sessions, StorageFault> => {
  const filename = options.filename;

  return filename === undefined || filename === ":memory:"
    ? memoryLayer(options)
    : sqliteLayer(filename, options);
};

/** The CLI store: `FOLD_AGENT_DB`, or the default file. */
export const configuredStoreLayer = Layer.unwrap(
  Effect.map(DatabasePath, (filename) => storeLayer({ filename })),
);
