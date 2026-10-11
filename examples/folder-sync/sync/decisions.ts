/** Server versions and feed cursors are separate, fixed-width lexical offsets. */
export interface KnownHead {
  readonly version: string | null;
  readonly hash: string | null;
  readonly localHash: string | null;
}

export const applyChange = (known: KnownHead | undefined, version: string) =>
  known?.version == null || version > known.version;

export const conflictBase = (known: KnownHead | undefined) => known?.version ?? null;

export const isEcho = (hash: string | null, known: KnownHead | undefined, pending = false) =>
  hash === (known?.localHash ?? null) || (!pending && hash === known?.hash);

export const feedReset = (cursor: string, tail: string) => cursor > tail;

export const encodeFilePath = (path: string) => path.split("/").map(encodeURIComponent).join("/");

export interface SyncOptions {
  readonly server: string;
  readonly directory: string;
  readonly folders?: ReadonlyArray<string>;
  readonly writable: ReadonlyArray<string>;
  /** Relative to directory. The host validates containment and disjoint folder roots. */
  readonly folderDirectory?: (folderId: string) => string;
  readonly settleMillis?: number;
}

export const folderDirectory = (folderId: string) => folderId;
