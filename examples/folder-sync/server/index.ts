import { BunHttpServer, BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Effect, Layer } from "effect";
import { ApiLive, HealthLive, NotFoundLive } from "./api.ts";
import { ServerConfig } from "./config.ts";
import { FolderStoreLive, Initialise, NoWriteHook } from "./folder-store.ts";
import { StorageLive } from "./storage.ts";

const Listener = Layer.unwrap(
  Effect.map(ServerConfig, ({ port }) =>
    BunHttpServer.layer({
      port,
      idleTimeout: 60,
      disablePreemptiveShutdown: true,
    }),
  ),
);

const Server = Serve.layer(Layer.mergeAll(ApiLive, HealthLive, NotFoundLive), {
  disableLogger: true,
}).pipe(
  Layer.provide(Listener),
  Layer.provide(Initialise),
  Layer.provide(FolderStoreLive),
  Layer.provide(NoWriteHook),
  Layer.provide(StorageLive),
);

BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });
