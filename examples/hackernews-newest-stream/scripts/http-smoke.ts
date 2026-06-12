import { resolve } from "node:path";

// Offline smoke test: stand up a tiny local fixture that mimics the Hacker News
// Firebase API, point the demo's poller at it via HN_API_BASE, then assert that
// the server-side TanStack DB -> createEffect projection emits client-readable
// Durable State events onto the Streamsy stream.

const packageDir = resolve(import.meta.dir, "..");
const demoPort = 20_000 + Math.floor(Math.random() * 20_000);
const fixturePort = demoPort + 1;
const baseUrl = `http://127.0.0.1:${demoPort}`;
const streamUrl = `${baseUrl}/streams/session/main`;

class SmokeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SmokeError";
  }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new SmokeError(message);
  }
}

interface FixtureStory {
  id: number;
  type: "story";
  by: string;
  time: number;
  title: string;
  score: number;
  descendants: number;
  url: string;
}

const baseTime = 1_700_000_000;
const fixtureStories: FixtureStory[] = [
  {
    id: 101,
    type: "story",
    by: "alice",
    time: baseTime + 30,
    title: "Streamsy ships durable streams",
    score: 42,
    descendants: 7,
    url: "https://example.com/a",
  },
  {
    id: 102,
    type: "story",
    by: "bob",
    time: baseTime + 20,
    title: "Materializers over event streams",
    score: 17,
    descendants: 3,
    url: "https://example.com/b",
  },
  {
    id: 103,
    type: "story",
    by: "carol",
    time: baseTime + 10,
    title: "TanStack DB on the server",
    score: 9,
    descendants: 1,
    url: "https://example.com/c",
  },
];
const fixtureById = new Map(fixtureStories.map((story) => [story.id, story]));

interface ChangeEvent {
  type: string;
  key: string;
  value: Record<string, unknown>;
  headers: { operation: string };
}

interface ApiStatus {
  projectionDisposed: boolean;
  lastPollCompletedAt?: string;
  lastPollError?: string;
  lastStoryCount: number;
}

// A minimal fixture HTTP server mimicking the HN Firebase v0 API.
const fixture = Bun.serve({
  port: fixturePort,
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/newstories.json") {
      return Response.json(fixtureStories.map((story) => story.id));
    }
    const itemMatch = url.pathname.match(/^\/item\/(\d+)\.json$/);
    if (itemMatch) {
      const story = fixtureById.get(Number(itemMatch[1]));
      return Response.json(story ?? null);
    }
    return new Response("not found", { status: 404 });
  },
});

async function waitForServer(): Promise<void> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(baseUrl);
      if (response.status === 200) return;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(100);
  }
  throw new SmokeError(`HN demo server did not become ready: ${lastError}`);
}

async function waitForPoll(): Promise<ApiStatus> {
  const deadline = Date.now() + 10_000;
  let last: ApiStatus | undefined;
  while (Date.now() < deadline) {
    const response = await fetch(`${baseUrl}/api/status`);
    if (response.ok) {
      last = (await response.json()) as ApiStatus;
      assert(!last.lastPollError, `poll reported an error: ${last.lastPollError}`);
      if (last.lastPollCompletedAt && last.lastStoryCount >= fixtureStories.length) {
        return last;
      }
    }
    await Bun.sleep(100);
  }
  throw new SmokeError(`HN demo did not complete a poll. Last status: ${JSON.stringify(last)}`);
}

async function waitForStreamEvents(): Promise<ChangeEvent[]> {
  const deadline = Date.now() + 10_000;
  let last: ChangeEvent[] = [];
  while (Date.now() < deadline) {
    const response = await fetch(`${streamUrl}?offset=-1`);
    if (response.status === 200) {
      last = (await response.json()) as ChangeEvent[];
      assert(Array.isArray(last), "stream read body should be a JSON array");
      const upserts = last.filter((event) => event.headers.operation === "upsert");
      if (upserts.length >= fixtureStories.length) return last;
    }
    await Bun.sleep(100);
  }
  throw new SmokeError(
    `Projection did not emit enough stream events. Got ${last.length}: ${JSON.stringify(last)}`,
  );
}

const server = Bun.spawn(["bun", "src/server/index.ts"], {
  cwd: packageDir,
  env: {
    ...process.env,
    PORT: String(demoPort),
    HN_API_BASE: `http://127.0.0.1:${fixturePort}`,
    HN_NEWEST_LIMIT: String(fixtureStories.length),
    // Large interval so only the startup poll runs; the test is deterministic.
    HN_POLL_INTERVAL_MS: "600000",
  },
  stdout: "pipe",
  stderr: "pipe",
});

try {
  await waitForServer();

  const status = await waitForPoll();
  assert(status.projectionDisposed === false, "projection should still be live after the poll");

  const events = await waitForStreamEvents();

  // Each fixture story must surface as a client-readable Durable State upsert event.
  for (const story of fixtureStories) {
    const match = events.find(
      (event) =>
        event.type === "hn-story" &&
        event.key === String(story.id) &&
        event.headers.operation === "upsert",
    );
    assert(match, `expected an hn-story upsert event for story ${story.id}`);
    assert(match.value.title === story.title, `event for story ${story.id} should carry its title`);
  }

  // Every event must be a well-formed change event a browser StreamDB can replay.
  for (const event of events) {
    assert(event.type === "hn-story", `unexpected event type ${event.type}`);
    assert(typeof event.key === "string" && event.key.length > 0, "event missing key");
    assert(event.value && typeof event.value === "object", "event missing value");
    assert(typeof event.headers?.operation === "string", "event missing headers.operation");
  }

  console.log(
    `hackernews-newest-stream HTTP smoke passed: ${status.lastStoryCount} polled, ${events.length} projected stream events`,
  );
} finally {
  server.kill();
  await server.exited.catch(() => undefined);
  fixture.stop(true);

  const stdout = await new Response(server.stdout).text();
  const stderr = await new Response(server.stderr).text();
  if (stdout.trim()) {
    console.log(stdout.trim());
  }
  if (stderr.trim()) {
    console.error(stderr.trim());
  }
}
