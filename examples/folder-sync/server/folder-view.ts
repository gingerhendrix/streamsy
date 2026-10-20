import { StreamAddress } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { File, Head } from "./domain.ts";
import { FileMessage } from "./projections.ts";
import { Files, FolderKey } from "./streams.ts";

/** The value of one live path and the txid of the change that wrote it. */
export const LiveFile = Schema.Struct({ txid: Schema.String, value: File });

export type LiveFile = typeof LiveFile.Type;

/**
 * What the folder owner decides from: the highest version in the folder and every
 * live path. It is a fold of the `Files` stream, so it cannot drift from the stream.
 */
export const FolderView = Schema.Struct({
  last: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  files: Schema.Record(Schema.String, LiveFile),
});

export type FolderView = typeof FolderView.Type;

export const headOf = ({ txid, value }: LiveFile): Head => ({
  version: value.version,
  hash: value.hash,
  txid,
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
  if (!("type" in message)) return { last: view.last, files: {} };
  const { [message.key]: _replaced, ...files } = view.files;

  if (!("value" in message)) return { last: view.last, files };
  // Every owner append stamps a txid. A replayed change without one answers with its version.
  const txid = message.headers.txid ?? String(message.value.version);

  return {
    last: Math.max(view.last, message.value.version),
    files: { ...files, [message.key]: { txid, value: message.value } },
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
      params: { folderId: FolderKey },
      // State's public change codec omits controls. Read the same stream through a JSON codec that accepts reset.
      inputs: {
        files: {
          ...StreamAddress.json(Files.at(params).id, { schema: FileMessage }),
          stream: Files,
        },
      },
      state: { schema: FolderView, initial: { last: 0, files: {} } },
      fold: (view, { item }) => applyFileMessage(view, item),
    }).at(params),
};
