import { BunRuntime } from "@effect/platform-bun";
import { drainOnShutdown, listener } from "@streamsy/serve/bun";
import { Effect, Layer } from "effect";
import { FetchHttpClient, HttpRouter } from "effect/unstable/http";
import { DemoConfig } from "./config.ts";
import { hackerNewsApiLayer } from "./hnews.ts";
import { app } from "./http.ts";
import { newestStoriesPollerLayer } from "./poller/poller.ts";
import { demoHostLayer } from "./streams.ts";

const pollerLayer = Layer.unwrap(
  Effect.map(DemoConfig, ({ newestLimit, pollIntervalMs }) =>
    newestStoriesPollerLayer({ limit: newestLimit, intervalMs: pollIntervalMs }),
  ),
).pipe(Layer.provide(hackerNewsApiLayer), Layer.provide(FetchHttpClient.layer));

const listenerLayer = Layer.unwrap(Effect.map(DemoConfig, ({ port }) => listener({ port })));

// On SIGINT or SIGTERM, runMain interrupts the launch. The drain answers live
// reads before the router closes, then the poller and storage close.
const serverLayer = HttpRouter.serve(app, { disableLogger: true }).pipe(
  drainOnShutdown(),
  Layer.provide(pollerLayer.pipe(Layer.provideMerge(demoHostLayer))),
  Layer.provide(listenerLayer),
);

BunRuntime.runMain(Layer.launch(serverLayer));
