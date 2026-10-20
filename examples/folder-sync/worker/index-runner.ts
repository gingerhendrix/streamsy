import { CheckpointStore, Transaction } from "@streamsy/projection";
import { Context, Effect, Layer, Schema, Semaphore } from "effect";
import { SqlClient } from "effect/sql";
import { BlobStore } from "../server/blob-store.ts";
import { runIndex } from "../server/index-followers.ts";
import { isUpsert } from "../server/messages.ts";
import { Files, Folders } from "../server/streams.ts";

export class IndexCatchupFailed extends Schema.TaggedError<IndexCatchupFailed>()(
  "IndexCatchupFailed",
  { message: Schema.String },
) {}

/**
 * The Worker index runs the same search projection as the Bun index, with the
 * same checkpoint and the same 410 rebuild. Only the source differs: `Files`
 * reads each folder object over HTTP. A ping or the hourly cron starts a pass.
 */
const make = Effect.gen(function* () {
  const registry = yield* Folders;
  const files = yield* Files;

  const context = yield* Effect.context<
    BlobStore | CheckpointStore | Files | SqlClient.SqlClient | Transaction
  >();

  // A pass reads over the network. One pass at a time keeps two checkpoint writes from racing.
  const gate = yield* Semaphore.make(1);

  /** A folder is registered before its stream exists, so a missing stream is not a failure. */
  const indexFolder = (folderId: string) =>
    runIndex(folderId).pipe(
      Effect.catchTag("IndexBootstrapUnavailable", (error) =>
        files.head({ folderId }).pipe(
          Effect.andThen(Effect.fail(error)),
          Effect.catchTag("StreamNotFound", () => Effect.void),
        ),
      ),
      Effect.provideContext(context),
    );

  const moved = (folderId: string) => gate.withPermit(indexFolder(folderId));

  const catchup = gate.withPermit(
    Effect.gen(function* () {
      let failed = false;

      for (const change of (yield* registry.collect({})).items) {
        if (!isUpsert(change)) continue;
        yield* indexFolder(change.value.folderId).pipe(
          Effect.catchCause((cause) => {
            failed = true;

            return Effect.logError(`Index catch-up failed for ${change.value.folderId}`, cause);
          }),
        );
      }

      if (failed)
        return yield* new IndexCatchupFailed({ message: "Some folders could not be indexed" });

      return yield* Effect.void;
    }),
  );

  return { moved, catchup };
});

export class IndexRunner extends Context.Service<IndexRunner, Effect.Success<typeof make>>()(
  "FolderSync/IndexRunner",
) {}

export const IndexRunnerLive = Layer.effect(IndexRunner, make);
