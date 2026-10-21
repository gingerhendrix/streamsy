import { Array } from "effect";

/** Server versions are folder sequences. Feed cursors are stream offsets. */
export interface KnownHead {
  readonly version: number | null;
  readonly hash: string | null;
  readonly localHash: string | null;
  readonly localVersion: number | null;
}

/** The parent of a local edit: the version that the disk copy is based on. */
export const editBase = (known: KnownHead | undefined) => known?.localVersion ?? null;

export const isEcho = (hash: string | null, known: KnownHead | undefined, pending = false) =>
  hash === (known?.localHash ?? null) || (!pending && hash === known?.hash);

/** A saved cursor past the stream tail means that the server lost history. */
export const historyRewound = (cursor: string, tail: string) => cursor !== "-1" && cursor > tail;

export interface SyncOptions {
  readonly server: string;
  readonly token?: string;
  readonly directory: string;
  readonly folders?: ReadonlyArray<string>;
  readonly writable: ReadonlyArray<string>;
  /** Relative to directory. The host validates containment and disjoint folder roots. */
  readonly folderDirectory?: (folderId: string) => string;
  readonly settleMillis?: number;
}

export const folderDirectory = (folderId: string) => folderId;

/** Common editor scratch files are never mirrored or uploaded. */
export const ignoredPath = (file: string) =>
  Array.some(
    file.split("/"),
    (name) =>
      name === ".DS_Store" ||
      name === "4913" ||
      /\.sw[px]$/.test(name) ||
      name.endsWith("~") ||
      name.startsWith(".#") ||
      (name.startsWith("#") && name.endsWith("#") && name.length > 1),
  );

/** Numbered backups retain the extension, including nested paths. */
export const backupPath = (file: string, suffix: number) => {
  if (suffix === 0) return file;
  const slash = file.lastIndexOf("/");
  const dot = file.lastIndexOf(".");
  const extension = dot > slash + 1 ? dot : file.length;

  return `${file.slice(0, extension)}-${suffix}${file.slice(extension)}`;
};
