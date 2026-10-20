import type { DurableObjectState } from "@cloudflare/workers-types";
import {
  CommitBoundary,
  DurableState,
  StorageFault,
  StreamAddress,
  Streamsy,
} from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, Context, DateTime, Effect, Layer, Schema, Semaphore } from "effect";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { SqlClient } from "effect/sql";
import { HealthLive, jsonBody } from "../server/api.ts";
import { BlobStore } from "../server/blob-store.ts";
import { CreateFolder, File, FolderConflict, FolderId } from "../server/domain.ts";
import { FileIndexLive } from "../server/file-index.ts";
import { IndexRequestLive } from "../server/index-api.ts";
import { applyIndexChanges, initialiseSearch, prepareIndexChanges } from "../server/projections.ts";
import { Folder, FolderKey, Folders } from "../server/streams.ts";
import type { Env } from "./env.ts";
import { objectStorage } from "./storage.ts";

const FileMessage = Schema.Union([
  StreamAddress.stateChange({ type: "file", schema: File }),
  Schema.Struct({ headers: Schema.Struct({ control: Schema.Literal("reset") }) }),
]);

class IndexReadFailure extends Schema.TaggedError<IndexReadFailure>()("IndexReadFailure", {
  message: Schema.String,
}) {}

const makeIndexReader = (env: Env) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const registry = yield* Folders;
    const blobs = yield* BlobStore;
    // A pass includes network reads. Serialize passes so an older response cannot replace a newer row.
    const gate = yield* Semaphore.make(1);

    const clear = (folderId: string) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`DELETE FROM folder_sync_manifest WHERE folderId = ${folderId}`;
          yield* sql`DELETE FROM folder_sync_search WHERE folderId = ${folderId}`;
          yield* sql`DELETE FROM folder_sync_cursors WHERE folderId = ${folderId}`;
        }),
      );

    const folder = Effect.fn("Index.readFolder")(function* (folderId: string) {
      const rows = yield* sql<{
        cursor: string;
      }>`SELECT cursor FROM folder_sync_cursors WHERE folderId = ${folderId}`;

      let cursor = rows[0]?.cursor ?? "-1";
      const key = yield* Schema.encodeEffect(FolderKey)(folderId);
      const stub = env.FOLDERS.get(env.FOLDERS.idFromName(folderId));

      while (true) {
        const response = yield* Effect.tryPromise({
          try: () =>
            stub.fetch(
              `https://internal/streams/folders/${key}/files?offset=${encodeURIComponent(cursor)}`,
            ),
          catch: (cause) => new IndexReadFailure({ message: String(cause) }),
        });

        // Registration precedes stream creation, so a crash can leave a registered empty folder.
        if (response.status === 404) return yield* Effect.void;

        if (response.status === 410) {
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

        if (messages.some((change) => !("type" in change))) {
          yield* clear(folderId);
          cursor = "-1";
          continue;
        }

        const next = response.headers.get("stream-next-offset");

        if (next === null)
          return yield* new IndexReadFailure({ message: `Missing cursor for ${folderId}` });

        const prepared = yield* prepareIndexChanges(
          messages.filter((change) => "type" in change),
        ).pipe(Effect.provideService(BlobStore, blobs));

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

    return { moved, catchup };
  });

class IndexReader extends Context.Service<
  IndexReader,
  Effect.Success<ReturnType<typeof makeIndexReader>>
>()("FolderSync/IndexReader") {}

const IndexInternal = Layer.mergeAll(
  HttpRouter.add(
    "POST",
    "/__register",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const input = yield* Schema.decodeUnknownEffect(CreateFolder)(yield* jsonBody(request));
      const registry = yield* Folders;
      const boundary = yield* CommitBoundary;

      const folder = yield* boundary.withTransaction(
        Effect.gen(function* () {
          const state = yield* registry.fold(
            {},
            () => new Map<string, Folder>(),
            (rows, change) => {
              if ("value" in change) rows.set(change.key, change.value);
              else rows.delete(change.key);

              return rows;
            },
          );

          const current = state.state.get(input.folderId);

          if (current !== undefined) {
            if (current.title !== input.title)
              return yield* new FolderConflict({ message: "Folder title differs" });

            return current;
          }

          const value = { folderId: input.folderId, title: input.title };

          const changes = DurableState.changes(Folders.at({}), { offset: state.offset }, [
            DurableState.upsert("folder", value, {
              headers: { txid: input.txid, timestamp: DateTime.formatIso(yield* DateTime.now) },
            }),
          ]);

          yield* registry.append({}, changes, { expectedOffset: state.offset });

          return value;
        }),
      );

      return HttpServerResponse.jsonUnsafe(folder);
    }).pipe(
      Effect.catchTags({
        FolderConflict: ({ message }) =>
          Effect.succeed(HttpServerResponse.jsonUnsafe({ error: message }, { status: 409 })),
        JsonBodyTooLarge: () =>
          Effect.succeed(
            HttpServerResponse.jsonUnsafe(
              { error: "JSON write bodies must be at most 512 KiB" },
              { status: 413 },
            ),
          ),
        SchemaError: () =>
          Effect.succeed(
            HttpServerResponse.jsonUnsafe({ error: "Invalid request" }, { status: 400 }),
          ),
        HttpServerError: () =>
          Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "Invalid body" }, { status: 400 })),
      }),
      Effect.orDie,
    ),
  ),
  HttpRouter.add(
    "POST",
    "/__moved",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;

      const input = yield* Schema.decodeUnknownEffect(Schema.Struct({ folderId: FolderId }))(
        yield* request.json,
      );

      yield* (yield* IndexReader).moved(input.folderId);

      return HttpServerResponse.text("indexed");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "POST",
    "/__catchup",
    Effect.gen(function* () {
      yield* (yield* IndexReader).catchup;

      return HttpServerResponse.text("indexed");
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("Index catch-up deferred", cause).pipe(
          Effect.as(
            HttpServerResponse.jsonUnsafe({ error: "Index catch-up deferred" }, { status: 503 }),
          ),
        ),
      ),
    ),
  ),
  HttpRouter.add(
    "POST",
    "/__test/index",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;

      const input = yield* Schema.decodeUnknownEffect(Schema.Struct({ dropPings: Schema.Boolean }))(
        yield* request.json,
      );

      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE folder_sync_test SET dropPings = ${Number(input.dropPings)} WHERE id = 1`;

      return HttpServerResponse.text("configured");
    }).pipe(Effect.orDie),
  ),
);

const indexInitialise = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_cursors (folderId TEXT PRIMARY KEY, cursor TEXT NOT NULL)`;
    yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_test (id INTEGER PRIMARY KEY, dropPings INTEGER NOT NULL)`;
    yield* sql`INSERT INTO folder_sync_test VALUES (1, 0) ON CONFLICT DO NOTHING`;
    yield* (yield* Folders).create({});
  }),
);

const indexLayer = (state: DurableObjectState, env: Env) =>
  Layer.mergeAll(FileIndexLive, Layer.effect(IndexReader, makeIndexReader(env))).pipe(
    Layer.provideMerge(indexInitialise),
    Layer.provideMerge(objectStorage(state, env, Streamsy.group(Folders))),
    Layer.catchCause((cause) =>
      Layer.effectContext(
        Effect.fail(
          new StorageFault({
            operation: "folder-sync.index.acquire",
            message: Cause.pretty(cause),
            retryable: true,
          }),
        ),
      ),
    ),
  );

const IndexApp = Layer.mergeAll(
  IndexRequestLive.pipe(Layer.provide(HttpServer.layerServices)),
  HealthLive,
  IndexInternal,
  Serve.mount(Folders, "/streams/folders"),
);

export class IndexObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof indexLayer>>
>({
  app: IndexApp,
  layer: indexLayer,
}) {}
