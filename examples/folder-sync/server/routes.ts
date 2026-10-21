import { Array, Schema } from "effect";
import { FolderKey } from "./streams.ts";

/** Encode each path segment and keep the slashes, so the router sees the folder structure. */
export const encodeFilePath = (path: string) =>
  Array.join(Array.map(path.split("/"), encodeURIComponent), "/");

/** The file route of one path: `PUT` commits, `DELETE` removes and `GET` reads the bytes. */
export const fileUrl = (folderId: string, path: string) =>
  `/api/folders/${encodeURIComponent(folderId)}/files/${encodeFilePath(path)}`;

/** The `Files` State stream of one folder. The segment is the hex UTF-8 folder key. */
export const folderStreamUrl = (folderId: string) =>
  `/streams/folders/${Schema.encodeSync(FolderKey)(folderId)}/files`;

/** The registry State stream of every folder. */
export const FOLDERS_STREAM_URL = "/streams/folders";

/** One immutable content-addressed blob. */
export const blobUrl = (hash: string) => `/api/blobs/${hash}`;

/** A stream read from `offset`, with an optional long-poll. */
export const streamRead = (url: string, offset: string, live = false) =>
  `${url}?offset=${encodeURIComponent(offset)}${live ? "&live=long-poll" : ""}`;
