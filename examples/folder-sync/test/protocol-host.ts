import { BunHttpServer } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { CheckpointStore, ProjectionFault } from "@streamsy/projection";
import { ConfigProvider, Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive } from "../server/api.ts";
import { FileIndexLive } from "../server/file-index.ts";
import { IndexLive } from "../server/index-live.ts";
import { FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { IndexApiLive } from "../server/index-api.ts";
import { Folders } from "../server/streams.ts";
import { mountFiles } from "../server/file-stream.ts";
import { sqliteHost } from "../server/storage.ts";

export const writerToken = "protocol-test-token";

export const protocolHost = (options: { failStorage?: () => boolean } = {}) => {
  const storage = sqliteHost(":memory:");

  // Every folder read and write loads the view checkpoint first.
  const checkpoints = Layer.effect(
    CheckpointStore,
    Effect.gen(function* () {
      const base = yield* CheckpointStore;

      return CheckpointStore.of({
        ...base,
        load: (key) =>
          options.failStorage?.()
            ? Effect.fail(
                new ProjectionFault({
                  phase: "load",
                  reason: "storage-failure",
                  message: "injected disk failure",
                }),
              )
            : base.load(key),
      });
    }),
  ).pipe(Layer.provide(storage));

  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(
      ApiLive,
      IndexApiLive,
      Serve.mount(Folders, "/streams/folders"),
      mountFiles(),
    ).pipe(
      Layer.provide(FileIndexLive),
      Layer.provide(IndexLive),
      Layer.provide(Initialise),
      Layer.provide(FolderStoreLive.pipe(Layer.provide(checkpoints))),
      HttpRouter.provideRequest(storage),
      Layer.provide([storage, BunHttpServer.layerHttpServices]),
      Layer.provide(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ FOLDER_SYNC_WRITER_TOKEN: writerToken })),
      ),
    ),
    { disableLogger: true },
  );

  return { fetch: (request: Request) => web.handler(request), stop: () => web.dispose() };
};
