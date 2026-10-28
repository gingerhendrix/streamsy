import { Projection } from "@streamsy/projection";
import { Effect, Layer } from "effect";
import { initialiseSearch, search } from "./search-index.ts";
import { Files } from "./streams.ts";

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
 * Local Files commits select their search binding. Each commit also writes a
 * pending mark in its transaction, so a restart runs the folders that a stop
 * left behind, removals included, without a scan of every folder.
 */
export const IndexLive = Projection.attach(search, { durable: true }).pipe(
  Layer.provide(Layer.effectDiscard(initialiseSearch)),
);
