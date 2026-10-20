import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault, Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { SqlClient } from "effect/sql";
import { HealthLive, jsonBody } from "../server/api.ts";
import { CreateFolder, FolderId } from "../server/domain.ts";
import { FileIndexLive } from "../server/file-index.ts";
import { registerFolder } from "../server/folder-registry.ts";
import { IndexRequestLive } from "../server/index-api.ts";
import { initialiseSearch } from "../server/search-index.ts";
import { IndexReader, indexReaderLayer } from "./index-reader.ts";
import { Folders } from "../server/streams.ts";
import type { Env } from "./env.ts";
import { objectStorage } from "./storage.ts";

const IndexInternal = Layer.mergeAll(
  HttpRouter.add(
    "POST",
    "/__register",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const input = yield* Schema.decodeUnknownEffect(CreateFolder)(yield* jsonBody(request));

      // The folder object creates the Files stream after this registration (P17).
      return HttpServerResponse.jsonUnsafe(yield* registerFolder(input));
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
  Layer.mergeAll(
    FileIndexLive,
    indexReaderLayer((folderId, key, cursor) =>
      env.FOLDERS.get(env.FOLDERS.idFromName(folderId)).fetch(
        `https://internal/streams/folders/${key}/files?offset=${encodeURIComponent(cursor)}`,
      ),
    ),
  ).pipe(
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
