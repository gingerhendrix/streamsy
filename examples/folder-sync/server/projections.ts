import { DurableState } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { Effect, Stream } from "effect";
import { SqlClient } from "effect/sql";
import { BlobStore } from "./blob-store.ts";
import { MissingFile, overwriteMarkFromOp } from "./domain.ts";
import { Changes, Files, FolderKey, Folders, Manifest, fileKey } from "./streams.ts";

/** A generation rebuild needs fresh output streams and an empty FTS table. Stop followers first. */
export const indexProjections = (generation = 1) => {
  const folderChanges = Projection.emit({
    id: "folder-changes",
    generation,
    params: { folderId: FolderKey },
    inputs: { input: Files },
    outputs: { output: Changes },
    commit: Commit.producer,
    emit: ({ item }, unit) => {
      const range = unit.ranges.input;

      if (range === undefined) throw new Error("Missing folder log range");

      return {
        output: [
          {
            folderId: unit.params.folderId,
            path: item.path,
            version: range.nextOffset,
            ...(item.type === "put" ? { hash: item.hash, size: item.size } : { size: 0 }),
            deleted: item.type === "delete",
            ...overwriteMarkFromOp(item),
            author: item.author,
            updatedAt: item.updatedAt ?? "1970-01-01T00:00:00.000Z",
          },
        ],
      };
    },
  });

  const manifest = Projection.emit({
    id: "manifest",
    generation,
    params: {},
    inputs: { input: Changes },
    outputs: { output: Manifest },
    commit: Commit.transaction,
    emit: ({ item }) => {
      const id = fileKey(item.folderId, item.path);

      if (item.deleted) return { output: [DurableState.remove("file", id)] };

      if (item.hash === undefined) throw new Error("Live change has no hash");

      return {
        output: [
          DurableState.upsert("file", {
            id,
            folderId: item.folderId,
            path: item.path,
            version: item.version,
            hash: item.hash,
            size: item.size,
            updatedAt: item.updatedAt,
            author: item.author,
          }),
        ],
      };
    },
  });

  const search = Projection.process({
    id: "search",
    generation,
    params: {},
    inputs: { input: Changes },
    commit: Commit.transaction,
    // Content lookup belongs outside the SQL commit. Immutable historical versions make retries safe.
    prepare: (batch) =>
      Effect.forEach(batch.input.items, (change) =>
        Effect.gen(function* () {
          if (change.deleted) return { change, text: undefined };
          const files = yield* Files;

          const entry = yield* files.read({ folderId: change.folderId }).pipe(
            Stream.filter((page) => page.nextOffset === change.version),
            Stream.take(1),
            Stream.runCollect,
          );

          const operation = entry[0]?.items[0];

          if (operation === undefined || operation.type !== "put" || operation.path !== change.path)
            return yield* new MissingFile({ message: "Change has no matching folder log entry" });

          if ("inline" in operation) return { change, text: operation.inline };
          const blobs = yield* BlobStore;
          const bytes = yield* blobs.get(operation.blob);

          if (bytes === undefined)
            return yield* new MissingFile({ message: "Indexed blob is missing" });

          return { change, text: searchableText(bytes) };
        }),
      ),
    process: (_batch, _unit, prepared) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        for (const { change, text } of prepared) {
          yield* sql`DELETE FROM folder_sync_search WHERE folderId = ${change.folderId} AND path = ${change.path}`;

          if (text !== undefined)
            yield* sql`INSERT INTO folder_sync_search (folderId, path, version, text) VALUES (${change.folderId}, ${change.path}, ${change.version}, ${text})`;
        }
      }),
  });

  return { folderChanges, manifest, search };
};

/** The same byte-preserving UTF-8 rule as inline content, without the inline size limit. */
export const searchableText = (bytes: Uint8Array): string | undefined => {
  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

    return text.includes("\0") ? undefined : text;
  } catch {
    return undefined;
  }
};

export const { folderChanges, manifest, search } = indexProjections();

export const initialiseSearch = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    "CREATE VIRTUAL TABLE IF NOT EXISTS folder_sync_search USING fts5(folderId UNINDEXED, path UNINDEXED, version UNINDEXED, text)",
  );
});

/** The caller owns runtime bindings; a new generation replays the registry during a rebuild. */
export const folderTriggers = <E, R>(
  start: (folderId: string) => Effect.Effect<void, E, R>,
  generation = 1,
) =>
  Projection.process({
    id: "folder-triggers",
    generation,
    params: {},
    inputs: { registry: Folders },
    commit: Commit.atLeastOnce,
    process: Projection.each(({ item }) => start(item.folderId)),
  });
