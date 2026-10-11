import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { FileOp, FolderState, Identifier, foldHead } from "./domain.ts";

/** Hex UTF-8 is an injective, canonical stream-id segment for opaque folder ids. */
export const FolderKey = Schema.StringFromHex.check(Schema.makeFilter(Schema.is(Identifier)));

export const Folder = Schema.Struct({
  folderId: Identifier,
  title: Identifier,
  created: Schema.String,
});

export type Folder = typeof Folder.Type;

export class Folders extends Streamsy.json<Folders>()("FolderSync/Folders", "folders", {
  params: {},
  schema: Folder,
}) {}

export class Files extends Streamsy.json<Files>()("FolderSync/Files", "folders/:folderId/files", {
  params: { folderId: FolderKey },
  schema: FileOp,
}) {}

export const streams = Streamsy.group(Folders, Files);

/** Hosts use readLimit: 1 so this range's end is the exact version of this entry. */
export const folderHead = Projection.fold({
  id: "folder-head",
  generation: 2,
  params: { folderId: FolderKey },
  inputs: { input: Files },
  state: { schema: FolderState, initial: { heads: {}, ops: {} } },
  fold: (state, { item }, unit) => {
    const range = unit.ranges.input;

    if (range === undefined) throw new Error("Missing folder log range");

    return foldHead(state, item, range.nextOffset);
  },
});
