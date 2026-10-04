import { BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { Config, Effect, Layer } from "effect";
import { Server } from "./server.ts";

const Listener = Layer.unwrap(
  Effect.map(Config.Port("PORT").pipe(Config.withDefault(1337)), (port) =>
    listener({ port, hostname: "::", idleTimeout: 60, gracefulShutdownTimeout: 1000 }),
  ),
);

BunRuntime.runMain(Layer.launch(Server.pipe(Layer.provide(Listener))), {
  teardown: Serve.teardown,
});
