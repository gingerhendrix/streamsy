import { StreamAddress } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { Array, Effect, Option, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { BlobStore } from "./blob-store.ts";
import { decodeText } from "./bytes.ts";
import { FileMessage, isLive, isReset } from "./messages.ts";
import { fileKey } from "./index-rows.ts";
import { Files, FolderKey } from "./streams.ts";

export const SEARCH_LIMIT = 1024 * 1024;

/** Stop the blob reader at the cap. Never buffer a large object for indexing. */
const blobText = Effect.fn("Search.blobText")(function* (hash: string) {
  const blobs = yield* BlobStore;
  const content = yield* blobs.get(hash);

  if (content === undefined) {
    yield* Effect.logWarning(`Indexed blob is missing; keeping metadata without text: ${hash}`);

    return undefined;
  }

  let length = 0;
  const bytes = new Uint8Array(Math.min(content.size, SEARCH_LIMIT));
  yield* content.body.pipe(
    Stream.tap((chunk) =>
      Effect.sync(() => {
        const prefix = chunk.subarray(0, bytes.length - length);
        bytes.set(prefix, length);
        length += prefix.length;
      }),
    ),
    Stream.takeUntil(() => length >= bytes.length),
    Stream.runDrain,
  );

  return decodeText(bytes.subarray(0, length), content.size > length);
});

/** Stage one prefix at a time outside the index transaction. Page size never multiplies JS text memory. */
export const prepareIndexChanges = Effect.fn("Search.prepare")(function* (
  folderId: string,
  changes: ReadonlyArray<FileMessage>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM folder_sync_index_text WHERE folderId = ${folderId}`;

  const lastReset = Option.getOrElse(Array.findLastIndex(changes, isReset), () => -1);

  for (const [position, change] of changes.entries()) {
    if (position < lastReset || !isLive(change)) continue;
    const text = yield* blobText(change.value.hash);

    if (text !== undefined)
      yield* sql`INSERT INTO folder_sync_index_text (folderId, position, text) VALUES (${folderId}, ${position}, ${text})`;
  }

  return changes;
});

export const clearIndexFolder = Effect.fn("Search.clear")(function* (folderId: string) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM folder_sync_search WHERE folderId = ${folderId}`;
  yield* sql`DELETE FROM folder_sync_manifest WHERE folderId = ${folderId}`;
});

export const applyIndexChanges = Effect.fn("Search.apply")(function* (
  folderId: string,
  prepared: ReadonlyArray<FileMessage>,
) {
  const sql = yield* SqlClient.SqlClient;

  for (const [position, change] of prepared.entries()) {
    if (StreamAddress.isControl(change)) {
      if (isReset(change)) yield* clearIndexFolder(folderId);
      continue;
    }

    yield* sql`DELETE FROM folder_sync_search WHERE folderId = ${folderId} AND path = ${change.key}`;
    yield* sql`DELETE FROM folder_sync_manifest WHERE folderId = ${folderId} AND path = ${change.key}`;

    // A tombstone removes the path, like a State delete.
    if (isLive(change)) {
      const value = change.value;

      const row = {
        id: fileKey(folderId, value.path),
        folderId,
        path: value.path,
        version: value.version,
        hash: value.hash,
        size: value.size,
        author: value.author,
        updatedAt: change.headers.timestamp ?? "1970-01-01T00:00:00.000Z",
      };

      yield* sql`INSERT INTO folder_sync_manifest ${sql.insert(row)}`;
      yield* sql`INSERT INTO folder_sync_search (folderId, path, version, text)
        SELECT ${folderId}, ${value.path}, ${value.version}, text FROM folder_sync_index_text
        WHERE folderId = ${folderId} AND position = ${position}`;
    }
  }

  yield* sql`DELETE FROM folder_sync_index_text WHERE folderId = ${folderId}`;
});

/** Each folder's SQL rows and input cursor commit together on the index owner. */
export const search = Projection.process({
  id: "folder-index",
  params: { folderId: FolderKey },
  inputs: { input: Files },
  commit: Commit.transaction,
  prepare: (batch, unit) => prepareIndexChanges(unit.params.folderId, batch.input.items),
  process: (batch, unit, prepared) =>
    batch.input.gone
      ? clearIndexFolder(unit.params.folderId)
      : applyIndexChanges(unit.params.folderId, prepared),
});

export const initialiseSearch = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_index_text (
    folderId TEXT NOT NULL, position INTEGER NOT NULL, text TEXT NOT NULL,
    PRIMARY KEY (folderId, position))`;
  yield* sql.unsafe(
    "CREATE VIRTUAL TABLE IF NOT EXISTS folder_sync_search USING fts5(folderId UNINDEXED, path UNINDEXED, version UNINDEXED, text)",
  );
  yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS folder_sync_manifest (
    id TEXT PRIMARY KEY, folderId TEXT NOT NULL, path TEXT NOT NULL, version INTEGER NOT NULL,
    hash TEXT NOT NULL, size INTEGER NOT NULL, author TEXT NOT NULL, updatedAt TEXT NOT NULL)`);
});
