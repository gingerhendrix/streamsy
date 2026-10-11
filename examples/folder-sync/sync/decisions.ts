/** Server versions and feed cursors are separate, fixed-width lexical offsets. */
export interface KnownHead {
  readonly version: string | null;
  readonly hash: string | null;
  readonly localHash: string | null;
  readonly localVersion: string | null;
}

export const applyChange = (known: KnownHead | undefined, version: string) =>
  known?.version == null || version > known.version;

export const conflictBase = (known: KnownHead | undefined) => known?.localVersion ?? null;

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

/** Keep the extension and use a stable, path-safe origin in sibling conflict names. */
export const conflictPath = (file: string, origin: string, suffix = 0) => {
  const slash = file.lastIndexOf("/");
  const dot = file.lastIndexOf(".");
  const extension = dot > slash + 1 ? dot : file.length;
  const id = origin.replace(/[^a-zA-Z0-9-]/g, "_");

  return `${file.slice(0, extension)}.conflict-${id}${suffix === 0 ? "" : `-${suffix}`}${file.slice(extension)}`;
};

/** Common editor scratch files are never mirrored or uploaded. */
export const ignoredPath = (file: string) =>
  file
    .split("/")
    .some(
      (name) =>
        name === ".DS_Store" ||
        name === "4913" ||
        /\.sw[px]$/.test(name) ||
        name.endsWith("~") ||
        name.startsWith(".#") ||
        (name.startsWith("#") && name.endsWith("#") && name.length > 1),
    );
