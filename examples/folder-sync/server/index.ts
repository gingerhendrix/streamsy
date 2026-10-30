import { BunHttpServer, BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
import index from "../public/web/index.html";
import { ApiLive, HealthLive } from "./api.ts";
import { ServerConfig } from "./config.ts";
import { FolderStoreLive, Initialise } from "./folder-store.ts";
import { IndexApiLive } from "./index-api.ts";
import { FileIndexLive } from "./file-index.ts";
import { IndexLive } from "./index-live.ts";
import { WakeLive } from "./wake.ts";
import { Folders, FolderWake } from "./streams.ts";
import { mountFiles } from "./file-stream.ts";
import { StorageLive } from "./storage.ts";

const Listener = Layer.unwrap(
  Effect.map(ServerConfig, ({ port }) =>
    BunHttpServer.layer({
      port,
      idleTimeout: 60,
      development: false,
      disablePreemptiveShutdown: true,
      routes: {
        "/api": false,
        "/api/*": false,
        "/streams": false,
        "/streams/*": false,
        "/health": false,
        "/*": index,
      },
    }),
  ),
);

const Server = Serve.layer(
  Layer.mergeAll(
    ApiLive,
    IndexApiLive,
    HealthLive,
    Serve.mount(Folders, "/streams/folders"),
    Serve.mount(FolderWake, "/streams/wake"),
    mountFiles(),
  ),
).pipe(
  Layer.provide(Listener),
  Layer.provide(FileIndexLive),
  Layer.provide(IndexLive),
  Layer.provide(WakeLive),
  Layer.provide(Initialise),
  Layer.provide(FolderStoreLive),
  Layer.provide(StorageLive),
);

BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });
