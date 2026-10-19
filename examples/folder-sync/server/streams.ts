import { Streamsy, type StreamAddress } from "@streamsy/core";
import { Schema } from "effect";
import { File, FolderId, Identifier } from "./domain.ts";

/** Hex UTF-8 is an injective, canonical stream-id segment for opaque folder ids. */
export const FolderKey = Schema.StringFromHex.check(Schema.makeFilter(Schema.is(FolderId)));

export const Folder = Schema.Struct({ folderId: FolderId, title: Identifier });

export type Folder = typeof Folder.Type;

export class Folders extends Streamsy.state<Folders>()("FolderSync/Folders", "folders", {
  params: {},
  collections: { folder: { schema: Folder, key: "folderId" } },
}) {}

export class Files extends Streamsy.state<Files>()("FolderSync/Files", "folders/:folderId/files", {
  params: { folderId: FolderKey },
  collections: { file: { schema: File, key: "path" } },
}) {}

export type FileChange = StreamAddress.CollectionsChange<typeof Files.collections>;

export type FolderChange = StreamAddress.CollectionsChange<typeof Folders.collections>;

export const streams = Streamsy.group(Folders, Files);

/** The public ls row keeps its existing shape, independently of the source streams. */
export const ListedFile = Schema.Struct({
  id: Schema.String,
  folderId: FolderId,
  ...File.fields,
  updatedAt: Schema.String,
});

export type ListedFile = typeof ListedFile.Type;

export const fileKey = (folderId: string, path: string) => JSON.stringify([folderId, path]);
