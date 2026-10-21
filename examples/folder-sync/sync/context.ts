import { Array, Crypto, Effect, FileSystem, Order, Path, Schema, Semaphore } from "effect";
import { SyncRemote } from "./client.ts";
import { folderDirectory, type SyncOptions } from "./decisions.ts";
import { LocalFailure, skipWarnings, SkippedLocal } from "./local.ts";
import { SyncState } from "./state.ts";

/**
 * What every part of one sync run shares: the services, the mirror root, the
 * folder roots and the gate. The gate is the one owner of filesystem
 * observations and durable cursor settlement. Network tails wait outside it.
 */
export interface SyncContext {
  readonly options: SyncOptions;
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly crypto: Crypto.Crypto;
  readonly state: typeof SyncState.Service;
  readonly remote: typeof SyncRemote.Service;
  /** The absolute mirror directory. */
  readonly root: string;
  readonly gate: Semaphore.Semaphore;
  /** Folder roots relative to `root`, filled as folders are added. */
  readonly roots: Map<string, string>;
  readonly warn: (file: string, message: string) => Effect.Effect<void>;
}

export const openContext = Effect.fn("Sync.context")(function* (options: SyncOptions) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const state = yield* SyncState;
  const remote = yield* SyncRemote;

  const configuration = yield* Schema.encodeEffect(
    Schema.fromJsonString(
      Schema.Struct({
        server: Schema.String,
        folders: Schema.NullOr(Schema.Array(Schema.String)),
        writable: Schema.Array(Schema.String),
      }),
    ),
  )({
    server: options.server,
    folders: options.folders === undefined ? null : Array.sort([...options.folders], Order.String),
    writable: Array.sort([...options.writable], Order.String),
  });

  const stored = yield* state.get("configuration");

  if (stored !== undefined && stored !== configuration)
    return yield* new LocalFailure({
      message: "This mirror belongs to another server or selection. Use another --dir.",
    });
  yield* state.set("configuration", configuration);
  yield* state.set("ready", "false");

  const context: SyncContext = {
    options,
    fs,
    path,
    crypto,
    state,
    remote,
    root: path.resolve(options.directory),
    gate: yield* Semaphore.make(1),
    roots: new Map(),
    warn: yield* skipWarnings,
  };

  return context;
});

export const selected = (context: SyncContext, folderId: string) =>
  context.options.folders === undefined || context.options.folders.includes(folderId);

export const writable = (context: SyncContext, folderId: string) =>
  context.options.writable.includes(folderId);

/** The configured directory of a folder, relative to the mirror root. */
export const mappedDirectory = (context: SyncContext, folderId: string) =>
  (context.options.folderDirectory ?? folderDirectory)(folderId);

/** The directory of a folder, relative to the mirror root. */
export const folderPath = (context: SyncContext, folderId: string) =>
  context.roots.get(folderId) ?? mappedDirectory(context, folderId);

/** One file of a folder, relative to the mirror root. */
export const filePath = (context: SyncContext, folderId: string, file: string) =>
  context.path.join(folderPath(context, folderId), file);

/** Log a skipped local path once and go on. */
export const skip =
  (context: SyncContext) =>
  <A, E, R>(work: Effect.Effect<A, E, R>) =>
    work.pipe(
      Effect.catchIf(Schema.is(SkippedLocal), (error) => context.warn(error.path, error.message)),
    );

/** The first candidate name, by suffix 0, 1, 2 and so on, that is not taken. */
export const freePath = Effect.fn("Sync.freePath")(function* <E, R>(
  candidate: (suffix: number) => string,
  taken: (path: string) => Effect.Effect<boolean, E, R>,
) {
  let suffix = 0;
  let free = candidate(suffix);

  while (yield* taken(free)) free = candidate(++suffix);

  return free;
});
