import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault, Streamsy } from "@streamsy/core";
import { ProjectionWakeup } from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, ConfigProvider, Effect, Layer } from "effect";
import { HttpRouter, HttpServer, HttpServerResponse } from "effect/http";
import { FolderApiRequestLive, HealthLive } from "../server/api.ts";
import { CreateFolder } from "../server/domain.ts";
import { FolderStoreLive, initialiseFolder } from "../server/folder-store.ts";
import { mountFiles } from "../server/file-stream.ts";
import { jsonBody, WriterTokenLive } from "../server/http.ts";
import { Files, Folders, FolderWake } from "../server/streams.ts";
import { filesLink, wakeLink } from "./links.ts";
import { filesWake } from "../server/wake.ts";
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
  // No local `FolderWake`: the index object owns it, and `wakeLink` reaches it.
  // The host refuses to start the object if this group also holds it.
  const storage = objectStorage(state, env, Streamsy.group(Folders, Files));

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
  filesLink.routes,
);

export const folderConfiguration = {
  app: FolderApp,
  layer: folderLayer,
  links: (env: Env) => [wakeLink.client(env.INDEX)],
  wakeups: [ProjectionWakeup.attach(filesWake)],
};

/** Preserve the immutable blob length while streaming through workerd. */
export const fileResponse = (
  request: Request,
  response: Response,
  waitUntil: (work: Promise<unknown>) => void,
): Response => {
  const match = /^\/api\/folders\/([^/]+)\/files\//.exec(new URL(request.url).pathname);
  // Workerd only preserves Content-Length on streams whose byte length it can prove.
  // The shared file API knows the immutable blob size, so retain streaming and its length.
  const length = response.headers.get("content-length");

  if (match && request.method === "GET" && response.ok && response.body && length !== null) {
    const bounded = new FixedLengthStream(Number(length));
    waitUntil(response.body.pipeTo(bounded.writable));

    return new Response(bounded.readable, response);
  }

  return response;
};

export class FolderObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof folderLayer>>,
  FolderWake
>(folderConfiguration) {
  // oxlint-disable-next-line effecttsgo/async-function -- Native Durable Object response streaming boundary.
  override async fetch(request: Request): Promise<Response> {
    return fileResponse(request, await super.fetch(request), (work) => this.ctx.waitUntil(work));
  }
}
