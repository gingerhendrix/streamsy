import { Commit, Projection } from "@streamsy/projection";
import { Effect, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { BlobStore } from "./blob-store.ts";
import { MissingFile } from "./domain.ts";
import { Files, FolderKey, fileKey } from "./streams.ts";

export const SEARCH_LIMIT = 1024 * 1024;

export const searchableText = (bytes: Uint8Array): string | undefined => {
  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

    return text.includes("\0") ? undefined : text;
  } catch {
    return undefined;
  }
};

/** Stop the blob reader at the cap. Never buffer a large object for indexing. */
const blobText = Effect.fn("Search.blobText")(function* (hash: string) {
  const blobs = yield* BlobStore;
  const content = yield* blobs.get(hash);

  if (content === undefined) return yield* new MissingFile({ message: "Indexed blob is missing" });
  let length = 0;

  const chunks = yield* content.body.pipe(
    Stream.map((chunk) => {
      const prefix = chunk.subarray(0, SEARCH_LIMIT - length);
      length += prefix.length;

      return prefix;
    }),
    Stream.takeUntil(() => length >= SEARCH_LIMIT),
    Stream.runCollect,
  );

  const bytes = new Uint8Array(length);
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }

  return searchableText(bytes);
});

/** Each folder's SQL rows and input cursor commit together on the index owner. */
export const search = Projection.process({
  id: "folder-index",
  params: { folderId: FolderKey },
  inputs: { input: Files },
  commit: Commit.transaction,
  prepare: (batch) =>
    Effect.forEach(batch.input.items, (change) =>
      Effect.gen(function* () {
        const text =
          "value" in change
            ? (change.value.text ?? (yield* blobText(change.value.hash)))
            : undefined;

        return { change, text };
      }),
    ),
  process: (_batch, unit, prepared) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const folderId = unit.params.folderId;

      for (const { change, text } of prepared) {
        yield* sql`DELETE FROM folder_sync_search WHERE folderId = ${folderId} AND path = ${change.key}`;
        yield* sql`DELETE FROM folder_sync_manifest WHERE folderId = ${folderId} AND path = ${change.key}`;

        if ("value" in change) {
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

          if (text !== undefined)
            yield* sql`INSERT INTO folder_sync_search (folderId, path, version, text) VALUES (${folderId}, ${value.path}, ${value.version}, ${text})`;
        }
      }
    }),
});

export const initialiseSearch = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    "CREATE VIRTUAL TABLE IF NOT EXISTS folder_sync_search USING fts5(folderId UNINDEXED, path UNINDEXED, version UNINDEXED, text)",
  );
  yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS folder_sync_manifest (
    id TEXT PRIMARY KEY, folderId TEXT NOT NULL, path TEXT NOT NULL, version INTEGER NOT NULL,
    hash TEXT NOT NULL, size INTEGER NOT NULL, author TEXT NOT NULL, updatedAt TEXT NOT NULL)`);
});
