/**
 * A minimal long-running host for the `Serve.layer` spawn tests: the protocol
 * routes over memory storage, served through `Serve.layer`, and launched with
 * `Serve.teardown`. The listener and `runMain` come from `./runtime.ts`.
 */
import { Config, Effect, Layer } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { ShutdownSignal } from "@streamsy/core/http";
import { Streams, routes } from "@streamsy/core/protocol";
import { Serve } from "../../src/index.ts";
import { listener, runMain } from "./runtime.ts";

// Reports the live-read count, so the test sends SIGTERM only once its poll is parked.
const Active = HttpRouter.add(
  "GET",
  "/active",
  Effect.gen(function* () {
    const signal = yield* ShutdownSignal;
    return HttpServerResponse.text(String(yield* signal.active));
  }),
);

const Listener = Layer.unwrap(Effect.map(Config.Port("PORT"), (port) => listener({ port })));

const Server = Serve.layer(Layer.mergeAll(routes({ prefix: "/streams" }), Active), {
  disableLogger: true,
  disableListenLog: true,
}).pipe(Layer.provide(Streams.layerMemory({ longPollTimeoutMs: 30_000 })), Layer.provide(Listener));

runMain(Layer.launch(Server), { teardown: Serve.teardown });
