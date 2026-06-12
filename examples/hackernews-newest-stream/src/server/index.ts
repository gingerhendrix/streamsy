import {
  newestLimit,
  pollIntervalMs,
  port,
  serverIdleTimeoutSeconds,
  streamPath,
} from "./config.ts";
import { json } from "./http.ts";
import { NewestStoriesPoller } from "./newest-poller.ts";
import { createHnServerDb } from "./server-db.ts";
import { startNewestProjection } from "./stream-projection.ts";
import { DemoStreams } from "./streams.ts";
import { serveStatic } from "./static.ts";

const streams = new DemoStreams();
await streams.start();

const serverDb = createHnServerDb();
const projection = startNewestProjection({
  storiesCollection: serverDb.storiesCollection,
  streams,
});

const poller = new NewestStoriesPoller({
  limit: newestLimit,
  intervalMs: pollIntervalMs,
  writer: serverDb.storiesWriter,
});
poller.start();

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
          newestLimit,
          pollIntervalMs,
          projectionDisposed: projection.disposed,
          ...poller.stats(),
        });
      }
      if (url.pathname === "/api/poll" && request.method === "POST") {
        void poller.pollNow();
        return json({ ok: true });
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

process.on("SIGINT", async () => {
  poller.stop();
  await projection.dispose();
  server.stop();
  process.exit(0);
});

console.log(`Hacker News newest stream demo listening on http://localhost:${server.port}`);
console.log(`Streamsy durable state stream: http://localhost:${server.port}${streamPath}`);
console.log(`Polling HN newest ${newestLimit} every ${pollIntervalMs}ms`);

export { server };
