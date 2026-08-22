/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console -- This Bun executable is the Promise-native HTTP/process edge; Effect-owned poller and projection work runs through the single ManagedRuntime below. */
import * as StateProjection from "@streamsy/experimental/state-projection";
import { ManagedRuntime } from "effect";
import {
  newestLimit,
  pollIntervalMs,
  port,
  projectionLimits,
  serverIdleTimeoutSeconds,
  sourceStreamPath,
  streamPath,
  streamPrefix,
} from "./config.ts";
import { json } from "./http.ts";
import { makeNewestStoriesPoller } from "./poller/poller.ts";
import { makeStoryProjection } from "./projection.ts";
import { serveStatic } from "./static.ts";
import { appendSourceBatchFromPromise, DemoStreams } from "./streams.ts";

const streams = new DemoStreams();
await streams.start();

const runtime = ManagedRuntime.make(StateProjection.layerClient(streams.client));
const projection = await runtime.runPromise(makeStoryProjection(projectionLimits));
const poller = await runtime.runPromise(
  makeNewestStoriesPoller({
    limit: newestLimit,
    intervalMs: pollIntervalMs,
    sink: {
      appendSourceBatch: appendSourceBatchFromPromise((changes) =>
        streams.appendSourceBatch(changes),
      ),
      catchUpProjection: projection.catchUp,
    },
  }),
);

const currentStats = () => ({
  projection: runtime.runSync(projection.status),
  ...runtime.runSync(poller.stats),
});

const server = Bun.serve({
  port,
  idleTimeout: serverIdleTimeoutSeconds,
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (url.pathname.startsWith(`${streamPrefix}/`)) {
        return streams.fetch(request);
      }
      if (url.pathname === "/api/status") {
        return json({
          streamPath,
          sourceStreamPath,
          newestLimit,
          pollIntervalMs,
          projectionLimits,
          ...currentStats(),
        });
      }
      if (url.pathname === "/api/poll" && request.method === "POST") {
        await runtime.runPromise(poller.pollNow);
        return json({ ok: true, ...currentStats() });
      }
      if (url.pathname.startsWith("/api/")) {
        return json({ error: "Not found" }, { status: 404 });
      }
      return serveStatic(url);
    } catch (error) {
      console.error(error);
      return json({ error: "Internal server error" }, { status: 500 });
    }
  },
});

await runtime.runPromise(poller.start);

let shuttingDown: Promise<void> | undefined;
function shutdown(): Promise<void> {
  if (shuttingDown) return shuttingDown;
  shuttingDown = (async () => {
    await server.stop(true);
    await runtime.runPromise(poller.stop);
    await runtime.dispose();
    await streams.close();
  })();
  return shuttingDown;
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown().finally(() => process.exit(0));
  });
}

console.log(`Hacker News newest stream demo listening on http://localhost:${server.port}`);
console.log(`Streamsy source stream: http://localhost:${server.port}${sourceStreamPath}`);
console.log(`Streamsy durable State stream: http://localhost:${server.port}${streamPath}`);
console.log(`Polling HN newest ${newestLimit} every ${pollIntervalMs}ms`);

export { server, shutdown };
