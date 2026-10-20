import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http";
import { ApiRequestLive, HealthLive, jsonBody } from "../server/api.ts";
import { CreateFolder } from "../server/domain.ts";
import { FolderStoreLive, initialiseFolder } from "../server/folder-store.ts";
import { mountFiles } from "../server/file-stream.ts";
import { objectStorage } from "./storage.ts";
import type { Env } from "./env.ts";

class TestHead extends Context.Service<TestHead, { fail: boolean }>()("FolderSync/TestHead") {}

const FolderInternal = Layer.mergeAll(
  HttpRouter.add(
    "POST",
    "/__create",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const body = yield* Schema.decodeUnknownEffect(CreateFolder)(yield* jsonBody(request));
      yield* initialiseFolder(body.folderId);

      return HttpServerResponse.jsonUnsafe({ folderId: body.folderId, title: body.title });
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "POST",
    "/__test/head",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;

      const body = yield* Schema.decodeUnknownEffect(Schema.Struct({ fail: Schema.Boolean }))(
        yield* jsonBody(request),
      );

      const flags = yield* TestHead;
      flags.fail = body.fail;

      return HttpServerResponse.text("configured");
    }).pipe(Effect.orDie),
  ),
);

const folderLayer = (state: DurableObjectState, env: Env) => {
  const storage = objectStorage(state, env);
  const flags = Layer.sync(TestHead, () => ({ fail: false }));

  const records = Layer.effect(
    OwnerRecords,
    Effect.gen(function* () {
      const base = yield* OwnerRecords;
      const test = yield* TestHead;

      return OwnerRecords.of({
        ...base,
        write: (key, value) =>
          env.TEST_HOOKS === "true" && test.fail
            ? Effect.fail(
                new StorageFault({
                  operation: "records.write",
                  message: "Injected head persistence failure",
                  retryable: false,
                }),
              )
            : base.write(key, value),
      });
    }),
  ).pipe(Layer.provide([storage, flags]));

  return FolderStoreLive.pipe(
    Layer.provideMerge(records),
    Layer.provideMerge(flags),
    Layer.provideMerge(storage),
    Layer.provideMerge(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({ FOLDER_SYNC_WRITER_TOKEN: env.WRITER_TOKEN }),
      ),
    ),
    Layer.catchCause((cause) =>
      Layer.effectContext(
        Effect.fail(
          new StorageFault({
            operation: "folder-sync.acquire",
            message: Cause.pretty(cause),
            retryable: true,
          }),
        ),
      ),
    ),
  );
};

const FolderApp = Layer.mergeAll(
  ApiRequestLive.pipe(Layer.provide(HttpServer.layerServices)),
  HealthLive,
  FolderInternal,
  mountFiles(),
);

export class FolderObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof folderLayer>>
>({ app: FolderApp, layer: folderLayer }) {
  // oxlint-disable-next-line effecttsgo/async-function -- Native Durable Object boundary schedules a best-effort post-commit ping.
  override async fetch(request: Request): Promise<Response> {
    const response = await super.fetch(request);
    const match = /^\/api\/folders\/([^/]+)\/files\//.exec(new URL(request.url).pathname);

    if (response.ok && ["PUT", "DELETE"].includes(request.method) && match?.[1]) {
      const folderId = decodeURIComponent(match[1]);
      this.ctx.waitUntil(
        this.env.INDEX.get(this.env.INDEX.idFromName("index"))
          .fetch("https://internal/__moved", {
            method: "POST",
            body: JSON.stringify({ folderId }),
          })
          .then((result) => {
            if (!result.ok) throw new Error(`Index ping answered ${result.status}`);

            return result.text();
          })
          .catch((error) => {
            // oxlint-disable-next-line effecttsgo/global-console -- The cron backstop retries failed post-commit notifications.
            console.error(`Index ping failed for ${folderId}`, error);
          }),
      );
    }

    return response;
  }
}
