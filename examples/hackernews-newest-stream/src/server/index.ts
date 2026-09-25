/* oxlint-disable effecttsgo/async-function -- This executable owns process shutdown and the ManagedRuntime boundary. */
import { Cause, Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { listener } from "@streamsy/serve/bun";
import { newestLimit, pollIntervalMs, port, serverIdleTimeoutSeconds } from "./config.ts";
import { app } from "./http.ts";
import { newestStoriesPollerLayer } from "./poller/poller.ts";
import { demoHostLayer } from "./streams.ts";

const applicationLayer = newestStoriesPollerLayer({
  limit: newestLimit,
  intervalMs: pollIntervalMs,
}).pipe(Layer.provideMerge(demoHostLayer));
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
  return (shuttingDown ??= Effect.runPromise(
    runtime.disposeEffect.pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.failCause(cause),
      ),
    ),
  ));
}
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown().then(
      () => process.exit(0),
      (error) => {
        console.error(error);
        process.exit(1);
      },
    );
  });
}
