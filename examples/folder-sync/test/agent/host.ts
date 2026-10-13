import { BunHttpServer } from "@effect/platform-bun";
import { Layer } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive } from "../../server/api.ts";
import { FileIndexLive, IndexLive } from "../../server/file-index.ts";
import { FolderStoreLive, Initialise, NoWriteHook } from "../../server/folder-store.ts";
import { IndexApiLive } from "../../server/index-api.ts";
import { sqliteHost } from "../../server/storage.ts";

/** Each test owns an in-process Bun/SQLite HTTP host and its projection fibers. */
export const agentHost = () => {
  const storage = sqliteHost(":memory:");

  const web = HttpRouter.toWebHandler(
    Layer.merge(ApiLive, IndexApiLive).pipe(
      Layer.provide(FileIndexLive),
      Layer.provide(IndexLive),
      Layer.provide(Initialise),
      Layer.provide(FolderStoreLive),
      Layer.provide(NoWriteHook),
      HttpRouter.provideRequest(storage),
      Layer.provide([storage, BunHttpServer.layerHttpServices]),
    ),
    { disableLogger: true },
  );

  return { fetch: (request: Request) => web.handler(request), stop: () => web.dispose() };
};
