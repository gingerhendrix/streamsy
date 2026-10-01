import { BunRuntime, BunServices } from "@effect/platform-bun";
import { drainOnShutdown, listener } from "@streamsy/serve/bun";
import { Effect, FileSystem, Layer, Path } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { app } from "./app.ts";
import { ServerConfig } from "./config.ts";
import { applicationLayer } from "./host.ts";

const Server = Layer.unwrap(
  Effect.gen(function* () {
    const { port, databasePath, workspaces } = yield* ServerConfig;
    if (databasePath !== ":memory:") {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
    }
    return HttpRouter.serve(app(workspaces), { disableLogger: true }).pipe(
      drainOnShutdown(),
      Layer.provide(applicationLayer(databasePath, workspaces)),
      Layer.provideMerge(listener({ port })),
    );
  }),
).pipe(Layer.provide(BunServices.layer));

BunRuntime.runMain(Layer.launch(Server));
