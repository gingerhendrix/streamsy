import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault } from "@streamsy/core";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, ConfigProvider, Effect, Layer } from "effect";
import { HttpRouter, HttpServer, HttpServerResponse } from "effect/http";
import { FolderApiRequestLive, HealthLive } from "../server/api.ts";
import { CreateFolder } from "../server/domain.ts";
import { FolderStoreLive, initialiseFolder } from "../server/folder-store.ts";
import { mountFiles } from "../server/file-stream.ts";
import { jsonBody, WriterTokenLive } from "../server/http.ts";
import { indexObject } from "./placement.ts";
import { objectStorage } from "./storage.ts";
import type { Env } from "./env.ts";

const FolderInternal = HttpRouter.add(
  "POST",
  "/__create",
  Effect.gen(function* () {
    const body = yield* jsonBody(CreateFolder);
    yield* initialiseFolder(body.folderId);

    return HttpServerResponse.jsonUnsafe({ folderId: body.folderId, title: body.title });
  }).pipe(Effect.orDie),
);

const folderLayer = (state: DurableObjectState, env: Env) => {
  const storage = objectStorage(state, env);

  return Layer.merge(FolderStoreLive, WriterTokenLive).pipe(
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
  FolderApiRequestLive.pipe(Layer.provide(HttpServer.layerServices)),
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
        indexObject(this.env)
          .fetch("https://internal/__moved", {
            method: "POST",
            headers: { "content-type": "application/json" },
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

    // Workerd only preserves Content-Length on streams whose byte length it can prove.
    // The shared file API knows the immutable blob size, so retain streaming and its length.
    const length = response.headers.get("content-length");

    if (match && request.method === "GET" && response.ok && response.body && length !== null) {
      const bounded = new FixedLengthStream(Number(length));
      this.ctx.waitUntil(response.body.pipeTo(bounded.writable));

      return new Response(bounded.readable, response);
    }

    return response;
  }
}
