/* oxlint-disable effecttsgo/async-function -- This executable owns process shutdown and the ManagedRuntime boundary. */
import { Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { listener } from "@streamsy/serve/bun";
import {
  newestLimit,
  pollIntervalMs,
  port,
  projectionLimits,
  serverIdleTimeoutSeconds,
} from "./config.ts";
import { app } from "./http.ts";
import { newestStoriesPollerLayer } from "./poller/poller.ts";
import { storyProjectionLayer } from "./projection.ts";
import { demoHostLayer, DemoStreams } from "./streams.ts";

const pollerLayer = Layer.unwrap(
  Effect.gen(function* () {
    const streams = yield* DemoStreams;
    return newestStoriesPollerLayer({
      limit: newestLimit,
      intervalMs: pollIntervalMs,
      sink: {
        appendSourceBatch: streams.appendSourceBatch,
      },
    });
  }),
);
const applicationLayer = pollerLayer.pipe(
  Layer.provideMerge(storyProjectionLayer(projectionLimits)),
  Layer.provideMerge(demoHostLayer),
);
const runtime = ManagedRuntime.make(
  HttpRouter.serve(app, { disableLogger: true }).pipe(
    Layer.provide(applicationLayer),
    Layer.provide(
      listener({ port, idleTimeout: serverIdleTimeoutSeconds, gracefulShutdownTimeout: 1000 }),
    ),
  ),
);
await runtime.runPromise(Effect.void);

let shuttingDown: Promise<void> | undefined;
export function shutdown(): Promise<void> {
  return (shuttingDown ??= runtime.dispose());
}
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown().finally(() => process.exit(0));
  });
}
