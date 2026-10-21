import { StreamAddress } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { File, Head, isTombstone } from "./domain.ts";
import { FileMessage, isReset, isUpsert } from "./messages.ts";
import { Files, FolderKey } from "./streams.ts";

/** The retry memory keeps the answers of this many changes in each folder. */
export const RETRY_MEMORY = 1024;

/**
 * The metadata of one live path. The content is in the blob store under `hash`,
 * so the view grows by about 200 bytes for each file, whatever the file size is.
 */
export const LiveFile = Schema.Struct({
  txid: Schema.String,
  version: File.fields.version,
  parent: File.fields.parent,
  hash: File.fields.hash,
  size: File.fields.size,
  author: File.fields.author,
});

export type LiveFile = typeof LiveFile.Type;

/** One landed change in the retry memory. A tombstone has no hash. */
export const Landed = Schema.Struct({
  path: Schema.String,
  version: File.fields.version,
  parent: File.fields.parent,
  hash: Schema.NullOr(File.fields.hash),
});

export type Landed = typeof Landed.Type;

/**
 * What the folder owner decides from. It is a fold of the `Files` stream, so it
 * cannot drift from the stream. A `Map` holds the paths, because a path such as
 * `constructor` or `__proto__` is a member of every plain object.
 *
 * - `last` is the highest version in the folder.
 * - `files` holds every live path.
 * - `removed` holds the tombstone version of each path whose last change is a delete.
 * - `recent` maps the txid of each of the last `RETRY_MEMORY` changes to its answer,
 *   oldest first. It stops a retry from landing a second time.
 */
export const FolderView = Schema.Struct({
  last: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  files: Schema.ReadonlyMap(Schema.String, LiveFile),
  removed: Schema.ReadonlyMap(Schema.String, File.fields.version),
  recent: Schema.ReadonlyMap(Schema.String, Landed),
});

export type FolderView = typeof FolderView.Type;

export const emptyView = (): FolderView => ({
  last: 0,
  files: new Map(),
  removed: new Map(),
  recent: new Map(),
});

export const headOf = ({ txid, version, parent, hash }: LiveFile): Head => ({
  version,
  parent,
  hash,
  txid,
});

/** The file value of a live path. */
export const fileOf = (path: string, live: LiveFile): File => ({
  path,
  version: live.version,
  parent: live.parent,
  hash: live.hash,
  size: live.size,
  author: live.author,
});

/** Add one change to the retry memory and drop the oldest past the limit. */
const remember = (recent: FolderView["recent"], txid: string, landed: Landed) => {
  const next = new Map(recent);
  // A txid that fell out of the memory and landed again moves to the newest place.
  next.delete(txid);
  next.set(txid, landed);

  for (const oldest of next.keys()) {
    if (next.size <= RETRY_MEMORY) break;
    next.delete(oldest);
  }

  return next;
};

/**
 * One change applied to the view. A reset clears the paths and keeps `last` and
 * `recent`, so a version is never given twice and a retry of a write from before
 * the reset does not land again.
 */
export const applyFileMessage = (view: FolderView, message: FileMessage): FolderView => {
  if (StreamAddress.isControl(message))
    return isReset(message)
      ? { last: view.last, files: new Map(), removed: new Map(), recent: view.recent }
      : view;
  const files = new Map(view.files);
  const removed = new Map(view.removed);
  files.delete(message.key);
  removed.delete(message.key);

  // A plain State delete has no version. The owner never writes one.
  if (!isUpsert(message)) return { ...view, files, removed };
  const { headers, value } = message;
  // Every owner append stamps a txid. Only a change that the owner did not write lacks
  // one, and its answer is keyed by its version.
  const txid = headers.txid ?? String(value.version);
  const last = Math.max(view.last, value.version);

  if (isTombstone(value)) {
    removed.set(message.key, value.version);

    return {
      last,
      files,
      removed,
      recent: remember(view.recent, txid, {
        path: message.key,
        version: value.version,
        parent: value.parent,
        hash: null,
      }),
    };
  }

  files.set(message.key, {
    txid,
    version: value.version,
    parent: value.parent,
    hash: value.hash,
    size: value.size,
    author: value.author,
  });

  return {
    last,
    files,
    removed,
    recent: remember(view.recent, txid, {
      path: message.key,
      version: value.version,
      parent: value.parent,
      hash: value.hash,
    }),
  };
};

/**
 * The folder view at the tail of one folder's `Files` stream. Commands fold the
 * checkpoint and the tail in memory. A checkpoint that lags the tail is safe.
 */
export const folderView = {
  at: (params: { folderId: string }) =>
    Projection.fold({
      id: "folder-sync-view",
      // Version 3 adds parents, tombstones and the retry memory. An older checkpoint has
      // another key, so it is ignored.
      version: 3,
      params: { folderId: FolderKey },
      inputs: { files: Files },
      // The JSON codec stores each map as an array of [key, value] pairs, in insertion order.
      state: {
        schema: Schema.toCodecJson(FolderView),
        initial: emptyView,
      },
      fold: (view, { item }) => applyFileMessage(view, item),
    }).at(params),
};
