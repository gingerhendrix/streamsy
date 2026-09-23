/* oxlint-disable effecttsgo/async-function, effecttsgo/extends-native-error, effecttsgo/global-console, effecttsgo/global-date, effecttsgo/global-fetch, effecttsgo/global-random -- This offline Bun smoke is a single executable/platform boundary that drives child processes and HTTP fixtures through their native Promise APIs. */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- The Bun smoke owns one temporary SQLite directory and resolves the demo working directory at the process boundary.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Schema } from "effect";
import { createHnDb, type HnDb } from "../src/client/db.ts";
import { ApiStatusSmokeView, HackerNewsStateChange } from "../src/state-schema.ts";

// Offline vertical smoke: local HN fixture -> deterministic source batch ->
// scoped @streamsy/projection change watcher -> public target stream consumed by the browser,
// including a process restart against the same SQLite file.

const packageDir = resolve(import.meta.dir, "..");
const portReservation = Bun.serve({ port: 0, fetch: () => new Response("reserved") });
const demoPort = portReservation.port;
await portReservation.stop(true);
const baseUrl = `http://127.0.0.1:${demoPort}`;
const streamUrl = `${baseUrl}/state/newest`;
const scratchDir = mkdtempSync(join(tmpdir(), "streamsy-hn-smoke-"));
const databasePath = join(scratchDir, "hackernews.sqlite");

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
  port: 0,
  fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/newstories.json") return Response.json(newestIds);
    const itemMatch = url.pathname.match(/^\/item\/(\d+)\.json$/);
    if (itemMatch) return Response.json(fixtureById.get(Number(itemMatch[1])) ?? null);
    return new Response("not found", { status: 404 });
  },
});
const fixturePort = fixture.port;

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
        last.lastSourceOffset !== undefined &&
        last.projection.sourceThrough === last.lastSourceOffset
      ) {
        return last;
      }
    }
    await Bun.sleep(100);
  }
  throw new SmokeError(`HN demo did not converge. Last status: ${JSON.stringify(last)}`);
}

async function readStoryEvents(): Promise<readonly ChangeEvent[]> {
  const response = await fetch(`${streamUrl}?offset=-1`);
  assert(response.status === 200, `target stream read failed: ${response.status}`);
  assert(response.headers.get("x-streamsy-state-version") === "1", "served state version");
  return Schema.decodeUnknownSync(Schema.Array(HackerNewsStateChange))(await response.json());
}

function startDemo() {
  return Bun.spawn(["bun", "src/server/index.ts"], {
    cwd: packageDir,
    env: {
      ...process.env,
      PORT: String(demoPort),
      HN_API_BASE: `http://127.0.0.1:${fixturePort}`,
      HN_NEWEST_LIMIT: "2",
      HN_POLL_INTERVAL_MS: "600000",
      HN_DB: databasePath,
    },
    stdout: "pipe",
    stderr: "pipe",
  });
}

async function stopDemo(server: ReturnType<typeof startDemo>): Promise<string> {
  server.kill();
  await server.exited.catch(() => undefined);
  const stdout = await new Response(server.stdout).text();
  const stderr = await new Response(server.stderr).text();
  if (stdout.trim()) console.log(stdout.trim());
  if (stderr.trim()) console.error(stderr.trim());
  return stderr;
}

let server: ReturnType<typeof startDemo> | undefined = startDemo();
let serverStderr = "";
const sessions: HnDb[] = [];
async function session(): Promise<HnDb> {
  const db = createHnDb(baseUrl);
  sessions.push(db);
  await db.preload();
  return db;
}
async function waitForRows(db: HnDb, title: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const rows = Array.from(db.collections.stories.values());
    if (
      rows.length === 2 &&
      rows.some((row) => row.title === title) &&
      rows
        .map((row) => String(row.id))
        .sort()
        .join(",") === "101,103"
    )
      return;
    await Bun.sleep(20);
  }
  throw new SmokeError(
    `Collection did not converge: ${JSON.stringify(Array.from(db.collections.stories.values()))}`,
  );
}
try {
  await waitForServer();
  const initialStatus = await waitForStatus(1);
  const initialEvents = await readStoryEvents();
  assert(initialStatus.sourceChanges === 2, "initial poll should append two source changes");
  assert(initialEvents.length === 2, "initial projection should emit two story upserts");

  const live = await session();
  assert(live.collections.stories.size === 2, "new session replays two current rows from -1");
  const initialOffset = live.offset;
  const updated = fixtureById.get(101);
  assert(updated !== undefined, "fixture story 101 should exist");
  fixtureById.set(101, { ...updated, title: "Streamsy ships a complete projection", score: 43 });
  newestIds = [103, 101];
  const changedPoll = await fetch(`${baseUrl}/api/poll`, { method: "POST" });
  assert(changedPoll.ok, `changed poll failed: ${changedPoll.status}`);
  const changedStatus = await waitForStatus(2);
  const changedEvents = await readStoryEvents();
  await waitForRows(live, "Streamsy ships a complete projection");
  assert(live.offset !== initialOffset, "same session consumes the live suffix after poll");
  assert(
    !("old_value" in changedEvents.find((event) => event.headers.operation === "delete")!),
    "delete carries only a key",
  );
  const replay = await session();
  await waitForRows(replay, "Streamsy ships a complete projection");

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
        ("value" in event ? event.value.title : undefined) ===
          "Streamsy ships a complete projection",
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

  const sourceThroughBeforeRestart = unchangedStatus.projection.sourceThrough;
  assert(
    sourceThroughBeforeRestart !== undefined,
    "first process should expose its stored checkpoint offset",
  );
  serverStderr += await stopDemo(server);
  server = undefined;

  server = startDemo();
  await waitForServer();
  const restartedStatus = await waitForStatus(1);
  const restartedEvents = await readStoryEvents();
  assert(
    restartedStatus.projection.sourceThrough !== sourceThroughBeforeRestart,
    "projection checkpoint should continue after restart",
  );
  assert(
    restartedStatus.sourceChanges === 2,
    "restarted process should append only the two changes its own poll found",
  );
  assert(
    restartedEvents.length === unchangedEvents.length + 2,
    "a resumed checkpoint projects only the post-restart poll, never the retained prefix again",
  );
  assert(
    JSON.stringify(restartedEvents.slice(0, unchangedEvents.length)) ===
      JSON.stringify(unchangedEvents),
    "restart should preserve every target fact already stored",
  );
  const restartedSession = await session();
  await waitForRows(restartedSession, "Streamsy ships a complete projection");
  await waitForRows(live, "Streamsy ships a complete projection");
  const resumeDeadline = Date.now() + 10_000;
  while (live.offset !== restartedSession.offset && Date.now() < resumeDeadline)
    await Bun.sleep(20);
  assert(
    live.offset === restartedSession.offset,
    "pre-restart session resumes through fresh upserts",
  );

  const restartedUnchangedPoll = await fetch(`${baseUrl}/api/poll`, { method: "POST" });
  assert(restartedUnchangedPoll.ok, `post-restart poll failed: ${restartedUnchangedPoll.status}`);
  const restartedUnchangedStatus = await waitForStatus(1);
  const finalEvents = await readStoryEvents();
  assert(
    restartedUnchangedStatus.sourceBatches === 1,
    "unchanged post-restart poll must not append another source batch",
  );
  assert(finalEvents.length === restartedEvents.length, "unchanged post-restart poll must be idle");

  console.log(
    `hackernews-newest-stream HTTP smoke passed: ${finalEvents.length} State events; TanStack replay, live upsert/key-only delete, and SQLite restart resume`,
  );
} finally {
  for (const db of sessions) db.close();
  if (server !== undefined) serverStderr += await stopDemo(server);
  await fixture.stop(true);
  rmSync(scratchDir, { recursive: true, force: true });
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
