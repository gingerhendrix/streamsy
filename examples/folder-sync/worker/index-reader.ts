import { Context, Effect, Layer, Schema, Semaphore } from "effect";
import { SqlClient } from "effect/sql";
import { BlobStore } from "../server/blob-store.ts";
import {
  applyIndexChanges,
  clearIndexFolder,
  FileMessage,
  prepareIndexChanges,
} from "../server/projections.ts";
import { FolderKey, Folders } from "../server/streams.ts";

class IndexReadFailure extends Schema.TaggedError<IndexReadFailure>()("IndexReadFailure", {
  message: Schema.String,
}) {}

export const makeIndexReader = (
  read: (folderId: string, key: string, cursor: string) => Promise<Response>,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const registry = yield* Folders;
    const blobs = yield* BlobStore;
    // A pass includes network reads. Serialize passes so an older response cannot replace a newer row.
    const gate = yield* Semaphore.make(1);

    const clear = (folderId: string) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* clearIndexFolder(folderId);
          yield* sql`DELETE FROM folder_sync_cursors WHERE folderId = ${folderId}`;
        }),
      );

    const folder = Effect.fn("Index.readFolder")(function* (folderId: string) {
      const rows = yield* sql<{
        cursor: string;
      }>`SELECT cursor FROM folder_sync_cursors WHERE folderId = ${folderId}`;

      let cursor = rows[0]?.cursor ?? "-1";
      const key = yield* Schema.encodeEffect(FolderKey)(folderId);

      while (true) {
        const response = yield* Effect.tryPromise({
          try: () => read(folderId, key, cursor),
          catch: (cause) => new IndexReadFailure({ message: String(cause) }),
        });

        // Registration precedes stream creation, so a crash can leave a registered empty folder.
        if (response.status === 404) return yield* Effect.void;

        if (response.status === 410) {
          if (cursor === "-1")
            return yield* new IndexReadFailure({
              message: `History unavailable for ${folderId} at bootstrap`,
            });
          yield* clear(folderId);
          cursor = "-1";
          continue;
        }

        if (![200, 204].includes(response.status))
          return yield* new IndexReadFailure({
            message: `Folder ${folderId} answered ${response.status}`,
          });

        const messages =
          response.status === 204
            ? []
            : yield* Effect.tryPromise({
                try: () => response.json(),
                catch: (cause) => new IndexReadFailure({ message: String(cause) }),
              }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(FileMessage))));

        const next = response.headers.get("stream-next-offset");

        if (next === null)
          return yield* new IndexReadFailure({ message: `Missing cursor for ${folderId}` });

        const prepared = yield* prepareIndexChanges(folderId, messages).pipe(
          Effect.provideService(BlobStore, blobs),
        );

        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* applyIndexChanges(folderId, prepared).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            yield* sql`INSERT INTO folder_sync_cursors (folderId, cursor) VALUES (${folderId}, ${next}) ON CONFLICT (folderId) DO UPDATE SET cursor = excluded.cursor`;
          }),
        );

        if (response.headers.get("stream-up-to-date") === "true" || response.status === 204)
          return yield* Effect.void;

        if (next === cursor)
          return yield* new IndexReadFailure({ message: `Folder ${folderId} did not advance` });
        cursor = next;
      }
    });

    const moved = (folderId: string) =>
      gate.withPermit(
        Effect.gen(function* () {
          const flags = yield* sql<{
            dropPings: number;
          }>`SELECT dropPings FROM folder_sync_test WHERE id = 1`;

          if (flags[0]?.dropPings === 1) return yield* Effect.void;

          return yield* folder(folderId);
        }),
      );

    const catchup = gate.withPermit(
      Effect.gen(function* () {
        let failed = false;
        const rows = (yield* registry.collect({})).items;

        for (const change of rows) {
          if (!("value" in change)) continue;
          yield* folder(change.value.folderId).pipe(
            Effect.catchCause((cause) => {
              failed = true;

              return Effect.logError(`Index catch-up failed for ${change.value.folderId}`, cause);
            }),
          );
        }

        if (failed)
          return yield* new IndexReadFailure({ message: "Some folders could not be indexed" });

        return yield* Effect.void;
      }),
    );

    return {
      moved: (folderId: string) =>
        moved(folderId).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
      catchup: catchup.pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    };
  });

export class IndexReader extends Context.Service<
  IndexReader,
  Effect.Success<ReturnType<typeof makeIndexReader>>
>()("FolderSync/IndexReader") {}

export const indexReaderLayer = (read: Parameters<typeof makeIndexReader>[0]) =>
  Layer.effect(IndexReader, makeIndexReader(read));
