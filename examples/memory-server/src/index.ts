import { BunRuntime } from "@effect/platform-bun";
import { Http, Memory, Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { Config, Effect, Layer } from "effect";

const streams = Streamsy.group(Streamsy.Wildcard);

const Listener = Layer.unwrap(
  Effect.map(Config.Port("PORT").pipe(Config.withDefault(1337)), (port) =>
    listener({ port, hostname: "::", idleTimeout: 60, gracefulShutdownTimeout: 1000 }),
  ),
);

const Server = Serve.layer(Http.routes(streams, { access: "full" }), {
  disableLogger: true,
}).pipe(Layer.provide(Memory.layer(streams)), Layer.provide(Listener));

BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });
