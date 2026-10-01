/* oxlint-disable effecttsgo/node-builtin-import -- This executable example prepares its configured SQLite parent directory at the Bun edge. */
/** The store is a Layer. The same `Sessions` service runs on memory or on Bun SQLite. */
import { Memory, Streamsy, type StorageFault } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Config, Effect, Layer, Schema } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { Sessions } from "./session-log.ts";

export interface StoreOptions {
  readonly filename?: string;
  readonly longPollTimeoutMs?: number;
}
export class StorageNotAvailable extends Schema.TaggedError<StorageNotAvailable>()(
  "StorageNotAvailable",
  { message: Schema.String },
) {}

/** Next to the example, whatever the working directory is. */
export const DEFAULT_DATABASE_PATH = join(import.meta.dir, "..", ".data", "agent.sqlite");
export const DatabasePath = Config.String("FOLD_AGENT_DB").pipe(
  Config.withDefault(DEFAULT_DATABASE_PATH),
);

const sessions = Streamsy.group(Sessions);

export const memoryLayer = (options: { readonly longPollTimeoutMs?: number } = {}) =>
  Memory.layer(options, sessions);

const prepareDirectory = (filename: string) =>
  Effect.tryPromise({
    try: () => mkdir(dirname(filename), { recursive: true }),
    catch: (cause) =>
      new StorageNotAvailable({
        message: `Cannot prepare SQLite directory for ${filename}: ${String(cause)}`,
      }),
  });

export const sqliteLayer = (filename: string, options: StoreOptions = {}) =>
  Layer.unwrap(
    prepareDirectory(filename).pipe(
      Effect.as(
        BunStorage.layer(
          {
            client: { filename },
            ...(options.longPollTimeoutMs === undefined
              ? {}
              : { longPollTimeoutMs: options.longPollTimeoutMs }),
          },
          sessions,
        ),
      ),
    ),
  );

/** Memory for `:memory:` or no filename, Bun SQLite otherwise. */
export const storeLayer = (
  options: StoreOptions = {},
): Layer.Layer<Sessions, StorageFault | StorageNotAvailable> => {
  const filename = options.filename;
  return filename === undefined || filename === ":memory:"
    ? memoryLayer(options)
    : sqliteLayer(filename, options);
};

/** The CLI store: `FOLD_AGENT_DB`, or the default file. */
export const configuredStoreLayer = Layer.unwrap(
  Effect.map(DatabasePath, (filename) => storeLayer({ filename })),
);
