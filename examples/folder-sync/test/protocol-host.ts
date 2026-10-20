import { BunHttpServer } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { StorageFault } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import { ConfigProvider, Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { ApiLive } from "../server/api.ts";
import { FileIndexLive, IndexLive } from "../server/file-index.ts";
import { FolderStoreLive, Initialise } from "../server/folder-store.ts";
import { IndexApiLive } from "../server/index-api.ts";
import { Files, Folders } from "../server/streams.ts";
import { sqliteHost } from "../server/storage.ts";

export const writerToken = "protocol-test-token";

export const protocolHost = (options: { failWrites?: () => boolean } = {}) => {
  const storage = sqliteHost(":memory:");

  const records = Layer.effect(
    OwnerRecords,
    Effect.gen(function* () {
      const base = yield* OwnerRecords;

      return OwnerRecords.of({
        ...base,
        write: (key, value) =>
          options.failWrites?.()
            ? Effect.fail(
                new StorageFault({
                  operation: "write",
                  message: "injected disk failure",
                  retryable: false,
                }),
              )
            : base.write(key, value),
      });
    }),
  ).pipe(Layer.provide(storage));

  const web = HttpRouter.toWebHandler(
    Layer.mergeAll(
      ApiLive,
      IndexApiLive,
      Serve.mount(Folders, "/streams/folders"),
      Serve.mount(Files, "/streams/folders/:folderId/files"),
    ).pipe(
      Layer.provide(FileIndexLive),
      Layer.provide(IndexLive),
      Layer.provide(Initialise),
      Layer.provide(FolderStoreLive.pipe(Layer.provide(records))),
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
