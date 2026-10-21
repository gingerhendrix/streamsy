import { StreamAddress } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { File, Head } from "./domain.ts";
import { FileMessage, isReset, isUpsert } from "./messages.ts";
import { Files, FolderKey } from "./streams.ts";

/**
 * The metadata of one live path. The content is in the blob store under `hash`,
 * so the view grows by about 200 bytes for each file, whatever the file size is.
 */
export const LiveFile = Schema.Struct({
  txid: Schema.String,
  version: File.fields.version,
  hash: File.fields.hash,
  size: File.fields.size,
  author: File.fields.author,
});

export type LiveFile = typeof LiveFile.Type;

/**
 * What the folder owner decides from: the highest version in the folder and every
 * live path. It is a fold of the `Files` stream, so it cannot drift from the stream.
 * A `Map` holds the paths, because a path such as `constructor` or `__proto__` is
 * a member of every plain object.
 */
export const FolderView = Schema.Struct({
  last: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  files: Schema.ReadonlyMap(Schema.String, LiveFile),
});

export type FolderView = typeof FolderView.Type;

export const headOf = ({ txid, version, hash }: LiveFile): Head => ({ version, hash, txid });

/** The file value of a live path. */
export const fileOf = (path: string, live: LiveFile): File => ({
  path,
  version: live.version,
  hash: live.hash,
  size: live.size,
  author: live.author,
});

/**
 * One change applied to the view. A delete takes no number, and a reset keeps
 * `last`, so a version is never given twice.
 *
 * `last` is the highest version in the retained stream. A future State compaction
 * that drops deletes must keep this high-water mark, or a cold fold can give a
 * used version again.
 */
export const applyFileMessage = (view: FolderView, message: FileMessage): FolderView => {
  if (isReset(message)) return { last: view.last, files: new Map() };
  const files = new Map(view.files);
  files.delete(message.key);

  if (!isUpsert(message)) return { last: view.last, files };
  const { headers, value } = message;

  files.set(message.key, {
    // Every owner append stamps a txid. Only a change that the owner did not write lacks
    // one, and its head answers with its version.
    txid: headers.txid ?? String(value.version),
    version: value.version,
    hash: value.hash,
    size: value.size,
    author: value.author,
  });

  return { last: Math.max(view.last, value.version), files };
};

/**
 * The folder view at the tail of one folder's `Files` stream. Commands fold the
 * checkpoint and the tail in memory. A checkpoint that lags the tail is safe.
 */
export const folderView = {
  at: (params: { folderId: string }) =>
    Projection.fold({
      id: "folder-sync-view",
      // Version 2 holds metadata in a map. A version 1 checkpoint has another key, so it is ignored.
      version: 2,
      params: { folderId: FolderKey },
      // State's public change codec omits controls. Read the same stream through a JSON codec that accepts reset.
      inputs: {
        files: {
          ...StreamAddress.json(Files.at(params).id, { schema: FileMessage }),
          stream: Files,
        },
      },
      // The JSON codec stores the map as an array of [path, file] pairs.
      state: {
        schema: Schema.toCodecJson(FolderView),
        initial: () => ({ last: 0, files: new Map() }),
      },
      fold: (view, { item }) => applyFileMessage(view, item),
    }).at(params),
};
