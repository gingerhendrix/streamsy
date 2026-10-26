import { Projection } from "@streamsy/projection";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { afterLastReset, isUpsert } from "./messages.ts";
import { initialiseSearch, search } from "./search-index.ts";
import { Files, Folders } from "./streams.ts";

/**
 * Run one folder's search binding. A binding with no checkpoint whose source is
 * absent has nothing to index: the final gone unit already cleared its rows and
 * forgot it, or the folder never had an index.
 */
export const indexFolder = Effect.fn("Index.folder")(function* (folderId: string) {
  yield* Projection.run(search.at({ folderId })).pipe(
    Effect.asVoid,
    Effect.catchTag("ProjectionFault", (error) =>
      error.reason !== "history-unavailable"
        ? Effect.fail(error)
        : Effect.flatMap(Files, (files) => files.head({ folderId })).pipe(
            Effect.andThen(Effect.fail(error)),
            Effect.catchTags({ StreamNotFound: () => Effect.void, StreamGone: () => Effect.void }),
          ),
    ),
  );
});

/**
 * Bun keeps attach's pending work in memory, so a restart repairs it here. The
 * registry gives every registered folder, and the manifest gives every indexed
 * folder, which covers a removal that the stop interrupted. An up-to-date
 * folder costs one checkpoint read and one tail read.
 */
export const catchUpIndex = Effect.fn("Index.catchUp")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const folders = new Set<string>();

  for (const row of yield* sql<{
    folderId: string;
  }>`SELECT DISTINCT folderId FROM folder_sync_manifest`)
    folders.add(row.folderId);

  for (const change of afterLastReset((yield* (yield* Folders).collect({})).items))
    if (isUpsert(change)) folders.add(change.value.folderId);

  for (const folderId of folders) yield* indexFolder(folderId);
});

/** Local Files commits select their search binding; startup repairs missed in-memory work. */
export const IndexLive = Projection.attach(search).pipe(
  Layer.provide(Layer.effectDiscard(initialiseSearch.pipe(Effect.andThen(catchUpIndex())))),
);
