import { Streamsy, type StreamAddress } from "@streamsy/core";
import { Schema } from "effect";
import { FileValue, FolderId, Identifier } from "./domain.ts";

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
  collections: { file: { schema: FileValue, key: "path" } },
}) {}

/** One row for each folder `Files` stream: its id and tail. Projections write it; clients read. */
export class FolderWake extends Streamsy.wake<FolderWake>()("FolderSync/FolderWake", "wake", {
  params: {},
}) {}

export type FileChange = StreamAddress.CollectionsChange<typeof Files.collections>;

export type FolderChange = StreamAddress.CollectionsChange<typeof Folders.collections>;

export const streams = Streamsy.group(Folders, Files, FolderWake);
