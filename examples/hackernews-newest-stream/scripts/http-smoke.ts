/* oxlint-disable effecttsgo/async-function, effecttsgo/extends-native-error, effecttsgo/global-console, effecttsgo/global-date, effecttsgo/global-fetch, effecttsgo/global-random -- This offline Bun smoke is a single executable/platform boundary that drives child processes and HTTP fixtures through their native Promise APIs. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun smoke resolves the demo child-process working directory with the Node-compatible path API.
import { resolve } from "node:path";
import { Option, Schema } from "effect";
import { ApiStatusSmokeView, HackerNewsStateChange } from "../src/state-schema.ts";

// Offline vertical smoke: local HN fixture -> deterministic source batch ->
// bounded @streamsy/projection run -> public target stream consumed by the browser.

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

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new SmokeError(message);
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
  story(101, baseTime + 30, "Streamsy ships durable streams"),
  story(102, baseTime + 20, "Bounded projections over event streams"),
  story(103, baseTime + 40, "Durable State lineage"),
];
const fixtureById = new Map(fixtureStories.map((value) => [value.id, value]));
let newestIds = [101, 102];

type ChangeEvent = HackerNewsStateChange;
type ApiStatus = Schema.Schema.Type<typeof ApiStatusSmokeView>;

const fixture = Bun.serve({
  port: fixturePort,
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/newstories.json") return Response.json(newestIds);
    const itemMatch = url.pathname.match(/^\/item\/(\d+)\.json$/);
    if (itemMatch) return Response.json(fixtureById.get(Number(itemMatch[1])) ?? null);
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
  throw new SmokeError(`HN demo server did not become ready: ${String(lastError)}`);
}

async function waitForStatus(sourceBatches: number): Promise<ApiStatus> {
  const deadline = Date.now() + 10_000;
  let last: ApiStatus | undefined;
  while (Date.now() < deadline) {
    const response = await fetch(`${baseUrl}/api/status`);
    if (response.ok) {
      last = Schema.decodeUnknownSync(ApiStatusSmokeView)(await response.json());
      assert(!last.lastPollError, `poll reported an error: ${last.lastPollError}`);
      assert(!last.projection.lastError, `projection failed: ${last.projection.lastError}`);
      if (
        last.sourceBatches >= sourceBatches &&
        last.projection.lastOutcome?.status === "caught-up"
      ) {
        return last;
      }
    }
    await Bun.sleep(100);
  }
  throw new SmokeError(`HN demo did not converge. Last status: ${JSON.stringify(last)}`);
}

async function readStoryEvents(): Promise<ChangeEvent[]> {
  const response = await fetch(`${streamUrl}?offset=-1`);
  assert(response.status === 200, `target stream read failed: ${response.status}`);
  const values = Schema.decodeUnknownSync(Schema.Array(Schema.Unknown))(await response.json());
  const decodeChange = Schema.decodeUnknownOption(HackerNewsStateChange);
  return values.flatMap((value) => {
    const decoded = decodeChange(value);
    return Option.isSome(decoded) ? [decoded.value] : [];
  });
}

const server = Bun.spawn(["bun", "src/server/index.ts"], {
  cwd: packageDir,
  env: {
    ...process.env,
    PORT: String(demoPort),
    HN_API_BASE: `http://127.0.0.1:${fixturePort}`,
    HN_NEWEST_LIMIT: "2",
    HN_POLL_INTERVAL_MS: "600000",
  },
  stdout: "pipe",
  stderr: "pipe",
});

let serverStderr = "";
try {
  await waitForServer();
  const initialStatus = await waitForStatus(1);
  const initialEvents = await readStoryEvents();
  assert(initialStatus.sourceChanges === 2, "initial poll should append two source changes");
  assert(initialEvents.length === 2, "initial projection should emit two story upserts");

  const updated = fixtureById.get(101);
  assert(updated, "fixture story 101 should exist");
  fixtureById.set(101, { ...updated, title: "Streamsy ships a complete projection", score: 43 });
  newestIds = [103, 101];
  const changedPoll = await fetch(`${baseUrl}/api/poll`, { method: "POST" });
  assert(changedPoll.ok, `changed poll failed: ${changedPoll.status}`);
  const changedStatus = await waitForStatus(2);
  const changedEvents = await readStoryEvents();

  assert(changedStatus.lastStoryCount === 2, "bounded newest set should still contain two rows");
  assert(changedStatus.sourceChanges === 5, "changed poll should append delete plus two upserts");
  assert(
    changedEvents.some((event) => event.key === "102" && event.headers.operation === "delete"),
    "story leaving the newest set should emit a delete",
  );
  assert(
    changedEvents.some(
      (event) =>
        event.key === "101" &&
        event.headers.operation === "upsert" &&
        event.value?.title === "Streamsy ships a complete projection",
    ),
    "mutable story fields should emit an updated upsert",
  );
  assert(
    changedEvents.some((event) => event.key === "103" && event.headers.operation === "upsert"),
    "story entering the newest set should emit an upsert",
  );

  const unchangedPoll = await fetch(`${baseUrl}/api/poll`, { method: "POST" });
  assert(unchangedPoll.ok, `unchanged poll failed: ${unchangedPoll.status}`);
  const unchangedStatus = await waitForStatus(2);
  const unchangedEvents = await readStoryEvents();
  assert(unchangedStatus.sourceBatches === 2, "unchanged poll must not append a source batch");
  assert(
    unchangedEvents.length === changedEvents.length,
    "unchanged poll must not append projection output",
  );

  console.log(
    `hackernews-newest-stream HTTP smoke passed: ${unchangedStatus.sourceChanges} source changes, ${unchangedEvents.length} client-readable State events`,
  );
} finally {
  server.kill();
  await server.exited.catch(() => undefined);
  await fixture.stop(true);

  const stdout = await new Response(server.stdout).text();
  serverStderr = await new Response(server.stderr).text();
  if (stdout.trim()) console.log(stdout.trim());
  if (serverStderr.trim()) console.error(serverStderr.trim());
}

assert(serverStderr.trim().length === 0, `demo server wrote to stderr: ${serverStderr}`);

function story(id: number, time: number, title: string): FixtureStory {
  return {
    id,
    type: "story",
    by: `user-${id}`,
    time,
    title,
    score: id - 60,
    descendants: id - 95,
    url: `https://example.com/${id}`,
  };
}
