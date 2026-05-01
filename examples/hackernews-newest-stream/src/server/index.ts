import { newestLimit, pollIntervalMs, port, serverIdleTimeoutSeconds, streamPath } from "./config.ts";
import { json } from "./http.ts";
import { fetchNewestStories } from "./hnews.ts";
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

let lastPollStartedAt: string | undefined;
let lastPollCompletedAt: string | undefined;
let lastPollError: string | undefined;
let lastStoryCount = 0;
let polling = false;
let stopped = false;

async function pollNewestStories() {
  if (polling || stopped) return;
  polling = true;
  lastPollStartedAt = new Date().toISOString();
  lastPollError = undefined;

  try {
    const stories = await fetchNewestStories(newestLimit);
    serverDb.storiesWriter.upsertMany(stories);
    lastStoryCount = stories.length;
    lastPollCompletedAt = new Date().toISOString();
    console.log(`HN poll loaded ${stories.length} stories into the server TanStack DB`);
  } catch (error) {
    lastPollError = error instanceof Error ? error.message : String(error);
    console.error("HN poll failed", error);
  } finally {
    polling = false;
  }
}

void pollNewestStories();
const interval = setInterval(() => void pollNewestStories(), pollIntervalMs);

const server = Bun.serve({
  port,
  idleTimeout: serverIdleTimeoutSeconds,
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (url.pathname.startsWith("/streams/")) {
        return streams.proxy(request);
      }
      if (url.pathname === "/api/status") {
        return json({
          streamPath,
          newestLimit,
          pollIntervalMs,
          polling,
          projectionDisposed: projection.disposed,
          lastPollStartedAt,
          lastPollCompletedAt,
          lastPollError,
          lastStoryCount,
        });
      }
      if (url.pathname === "/api/poll" && request.method === "POST") {
        void pollNewestStories();
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
  stopped = true;
  clearInterval(interval);
  await projection.dispose();
  server.stop();
  process.exit(0);
});

console.log(`Hacker News newest stream demo listening on http://localhost:${server.port}`);
console.log(`Streamsy durable state stream: http://localhost:${server.port}${streamPath}`);
console.log(`Polling HN newest ${newestLimit} every ${pollIntervalMs}ms`);

export { server };
