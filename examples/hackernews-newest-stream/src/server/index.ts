import {
  newestLimit,
  pollIntervalMs,
  port,
  projectionLimits,
  serverIdleTimeoutSeconds,
  sourceStreamPath,
  streamPath,
} from "./config.ts";
import { json } from "./http.ts";
import { NewestStoriesPoller } from "./newest-poller.ts";
import { createStoryProjectionRuntime } from "./projection-runtime.ts";
import { serveStatic } from "./static.ts";
import { DemoStreams } from "./streams.ts";

const streams = new DemoStreams();
await streams.start();

const projection = createStoryProjectionRuntime(streams.client, projectionLimits);
const poller = new NewestStoriesPoller({
  limit: newestLimit,
  intervalMs: pollIntervalMs,
  sink: {
    appendSourceBatch: (changes) => streams.appendSourceBatch(changes),
    catchUpProjection: projection.catchUp,
  },
});

const server = Bun.serve({
  port,
  idleTimeout: serverIdleTimeoutSeconds,
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (url.pathname.startsWith("/streams/")) {
        return streams.fetch(request);
      }
      if (url.pathname === "/api/status") {
        return json({
          streamPath,
          sourceStreamPath,
          newestLimit,
          pollIntervalMs,
          projectionLimits,
          projection: projection.status(),
          ...poller.stats(),
        });
      }
      if (url.pathname === "/api/poll" && request.method === "POST") {
        await poller.pollNow();
        return json({ ok: true, projection: projection.status(), ...poller.stats() });
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

poller.start();

let shuttingDown: Promise<void> | undefined;
function shutdown(): Promise<void> {
  if (shuttingDown) return shuttingDown;
  shuttingDown = (async () => {
    server.stop(true);
    await poller.close();
    await projection.dispose();
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
