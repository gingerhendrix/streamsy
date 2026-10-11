import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault, Streamsy } from "@streamsy/core";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Serve } from "@streamsy/serve";
import { CheckpointStore, Projection, Transaction } from "@streamsy/projection";
import { CommitBoundary } from "@streamsy/core";
import { Cause, Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { ApiRequestLive, HealthLive, NotFoundLive } from "../server/api.ts";
import { FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { FileIndexLive } from "../server/file-index.ts";
import { BlobStore } from "../server/blob-store.ts";
import { IndexRequestLive } from "../server/index-api.ts";
import { feedEpoch } from "../server/feed-epoch.ts";
import { Folder, Folders, Files, Manifest, Changes } from "../server/streams.ts";
import { initialiseSearch, manifest, search } from "../server/projections.ts";
import { objectStorage } from "./storage.ts";
import {
  Notices,
  Notice,
  Delivery,
  OutboxLive,
  NoticeHook,
  deliveryLayer,
  noticeTables,
  receiveNotice,
  noticeChanges,
  notices,
} from "./notices.ts";
import { OutboxStore } from "@streamsy/serve/action/outbox";
import type { Env } from "./env.ts";

const unavailable = () =>
  HttpServerResponse.jsonUnsafe(
    { error: "Index unavailable; retry" },
    { status: 503, headers: { "retry-after": "1" } },
  );

const fault = (cause: Cause.Cause<unknown>) =>
  new StorageFault({
    operation: "folder-sync.acquire",
    message: Cause.pretty(cause),
    retryable: true,
  });

const FolderInternal = Layer.merge(
  HttpRouter.add(
    "POST",
    "/__drain",
    Effect.flatMap(Delivery, (delivery) => delivery.run).pipe(
      Effect.as(HttpServerResponse.text("drained")),
    ),
  ),
  HttpRouter.add(
    "GET",
    "/__pending",
    Effect.gen(function* () {
      const store = yield* OutboxStore;
      const rows = yield* store.list(notices.name, undefined).pipe(Effect.orDie);

      return HttpServerResponse.jsonUnsafe(
        rows.map(({ state, idempotencyKey }) => ({ state, idempotencyKey })),
      );
    }),
  ),
);

const folderLayer = (state: DurableObjectState, env: Env) =>
  Layer.mergeAll(FolderStoreLive, deliveryLayer(env)).pipe(
    Layer.provideMerge(Initialise),
    Layer.provideMerge(NoticeHook),
    Layer.provideMerge(OutboxLive),
    Layer.provideMerge(objectStorage(state, env)),
    Layer.catchCause((cause) => Layer.effectContext(Effect.fail(fault(cause)))),
  );

const FolderApp = Layer.mergeAll(
  ApiRequestLive.pipe(Layer.provide(HttpServer.layerServices)),
  HealthLive,
  NotFoundLive,
  FolderInternal,
);

export class FolderObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof folderLayer>>
>({ app: FolderApp, layer: folderLayer }) {
  // oxlint-disable-next-line effecttsgo/async-function -- Public stock-host fetch wrapper dispatches application routes on the same acquired owner.
  override async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (path === "/__drain" || path === "/__pending") return super.fetch(request);
    await super.fetch(new Request("https://internal/__drain", { method: "POST" }));
    const response = await super.fetch(request);
    await super.fetch(new Request("https://internal/__drain", { method: "POST" }));

    return response;
  }
}

class IndexPass extends Context.Service<IndexPass, { readonly run: Effect.Effect<void> }>()(
  "FolderSync/IndexPass",
) {}

const passLayer = Layer.effect(
  IndexPass,
  Effect.gen(function* () {
    const run = Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const flags = yield* sql<{
        failProjection: number;
      }>`SELECT failProjection FROM folder_sync_test WHERE id = 1`;

      if (flags[0]?.failProjection === 1) return yield* Effect.die("Injected projection failure");
      yield* Projection.run(noticeChanges.at({}));
      yield* Projection.run(manifest.at({}));
      yield* Projection.run(search.at({}));

      return yield* Effect.void;
    });

    const context = yield* Effect.context<
      | Notices
      | Changes
      | Manifest
      | Files
      | BlobStore
      | CheckpointStore
      | Transaction
      | CommitBoundary
      | SqlClient.SqlClient
    >();

    return IndexPass.of({ run: run.pipe(Effect.provideContext(context), Effect.orDie) });
  }),
);

const IndexInternal = Layer.mergeAll(
  HttpRouter.add(
    "GET",
    "/__project",
    Effect.gen(function* () {
      const pass = yield* IndexPass;
      yield* pass.run;

      return HttpServerResponse.text("ready", { headers: { "stream-epoch": yield* feedEpoch } });
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("Index projection failed; retry", cause).pipe(Effect.as(unavailable())),
      ),
    ),
  ),
  HttpRouter.add(
    "POST",
    "/__notice",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const sql = yield* SqlClient.SqlClient;
      const flags = yield* sql<{ fail: number }>`SELECT fail FROM folder_sync_test WHERE id = 1`;

      if (flags[0]?.fail === 1) return unavailable();
      const notice = yield* Schema.decodeUnknownEffect(Notice)(yield* request.json);
      yield* receiveNotice(notice);

      if (flags[0]?.fail === 2) return unavailable();
      // Persist intake before projecting. A failed response can safely replay the same notice.
      const pass = yield* IndexPass;
      yield* pass.run.pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Notice persisted; index projection deferred", cause),
        ),
      );

      return HttpServerResponse.text("accepted");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/__registry",
    Effect.gen(function* () {
      const registry = yield* Folders;

      return HttpServerResponse.jsonUnsafe((yield* registry.collect({})).items);
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "POST",
    "/__register",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const folder = yield* Schema.decodeUnknownEffect(Folder)(yield* request.json);
      const registry = yield* Folders;
      const boundary = yield* CommitBoundary;
      yield* boundary.withTransaction(
        Effect.gen(function* () {
          if (!(yield* registry.collect({})).items.some((row) => row.folderId === folder.folderId))
            yield* registry.append({}, [folder]);
        }),
      );

      return HttpServerResponse.text("registered");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "POST",
    "/__test/intake",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const sql = yield* SqlClient.SqlClient;

      const payload = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          fail: Schema.Boolean,
          projection: Schema.optionalKey(Schema.Boolean),
          afterPersist: Schema.optionalKey(Schema.Boolean),
        }),
      )(yield* request.json);

      const fail = payload.afterPersist ? 2 : Number(payload.fail);
      yield* sql`UPDATE folder_sync_test SET fail = ${fail}, failProjection = ${payload.projection ? 1 : 0} WHERE id = 1`;

      return HttpServerResponse.text("configured");
    }).pipe(Effect.orDie),
  ),
);

const indexInitialise = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    yield* noticeTables;
    yield* feedEpoch;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_test (id INTEGER PRIMARY KEY, fail INTEGER NOT NULL, failProjection INTEGER NOT NULL)`;
    yield* sql`INSERT INTO folder_sync_test VALUES (1, 0, 0) ON CONFLICT DO NOTHING`;
    yield* (yield* Notices).create({});
    yield* (yield* Changes).create({});
    yield* (yield* Manifest).create({});
    yield* (yield* Folders).create({});
  }),
);

const indexLayer = (state: DurableObjectState, env: Env) =>
  Layer.mergeAll(FileIndexLive, passLayer).pipe(
    Layer.provideMerge(indexInitialise),
    Layer.provideMerge(
      objectStorage(state, env, Streamsy.group(Folders, Files, Changes, Manifest, Notices)),
    ),
    Layer.catchCause((cause) => Layer.effectContext(Effect.fail(fault(cause)))),
  );

const IndexApp = Layer.mergeAll(
  IndexRequestLive.pipe(Layer.provide(HttpServer.layerServices)),
  HealthLive,
  IndexInternal,
  Serve.mount(Changes, "/streams/changes"),
  Serve.mount(Manifest, "/streams/manifest"),
);

export class IndexObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof indexLayer>>
>({ app: IndexApp, layer: indexLayer }) {
  // oxlint-disable-next-line effecttsgo/async-function -- Stock-host request wrapper runs the application projection route before reads.
  override async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;

    if (path === "/__project" || path === "/__test/intake" || path === "/__registry")
      return super.fetch(request);
    const ready = await super.fetch(new Request("https://internal/__project"));

    // Notice intake skips the read gate. Retain the best-effort prepass and post-intake pass.
    if (!ready.ok && path !== "/__notice") return ready;
    const response = await super.fetch(request);

    if (path !== "/streams/changes") return response;
    const headers = new Headers(response.headers);
    headers.set("stream-epoch", ready.headers.get("stream-epoch") ?? "");

    return new Response(response.body, { status: response.status, headers });
  }
}
