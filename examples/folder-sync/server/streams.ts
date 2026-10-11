import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import {
  FileOp,
  FolderId,
  FolderState,
  Identifier,
  Hash,
  Path,
  Version,
  foldHead,
} from "./domain.ts";

/** Hex UTF-8 is an injective, canonical stream-id segment for opaque folder ids. */
export const FolderKey = Schema.StringFromHex.check(Schema.makeFilter(Schema.is(FolderId)));

export const Folder = Schema.Struct({
  folderId: FolderId,
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

export const Change = Schema.Struct({
  folderId: Identifier,
  path: Path,
  version: Version,
  hash: Schema.optionalKey(Hash),
  size: Schema.Int,
  deleted: Schema.Boolean,
  inline: Schema.optionalKey(Schema.String),
  blob: Schema.optionalKey(Hash),
  overwrote: Schema.optionalKey(Version),
  author: Identifier,
  updatedAt: Schema.String,
});

export type Change = typeof Change.Type;

export class Changes extends Streamsy.json<Changes>()("FolderSync/Changes", "changes", {
  params: {},
  schema: Change,
}) {}

/** JSON tuples prevent collisions between opaque ids and paths. */
export const fileKey = (folderId: string, path: string) => JSON.stringify([folderId, path]);

export const ManifestFile = Schema.Struct({
  id: Schema.String,
  folderId: Identifier,
  path: Path,
  version: Version,
  hash: Hash,
  size: Schema.Int,
  updatedAt: Schema.String,
  author: Identifier,
});

export type ManifestFile = typeof ManifestFile.Type;

export class Manifest extends Streamsy.state<Manifest>()("FolderSync/Manifest", "manifest", {
  params: {},
  collections: { file: { schema: ManifestFile, key: "id" } },
}) {}

export const streams = Streamsy.group(Folders, Files, Changes, Manifest);

export const folderHead = Projection.fold({
  id: "folder-head",
  generation: 2,
  params: { folderId: FolderKey },
  inputs: { input: Files },
  state: { schema: FolderState, initial: { seq: 0, heads: {}, ops: {} } },
  fold: (state, { item }) => foldHead(state, item),
});
