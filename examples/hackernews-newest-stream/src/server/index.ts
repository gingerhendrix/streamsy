import { BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
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

const serverLayer = Serve.layer(app).pipe(
  Layer.provide(pollerLayer.pipe(Layer.provideMerge(demoHostLayer))),
  Layer.provide(listenerLayer),
);

BunRuntime.runMain(Layer.launch(serverLayer), { teardown: Serve.teardown });
