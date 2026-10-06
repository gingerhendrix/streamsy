import { BunHttpServer, BunRuntime, BunServices } from "@effect/platform-bun";
import { StreamAddress } from "@streamsy/core";
import { spawnServer } from "@streamsy/test-support/server";
import {
  Console,
  type Duration,
  Effect,
  FileSystem,
  Formatter,
  Layer,
  Path,
  Schedule,
  Schema,
} from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientResponse,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import { createHnDb, type HnDb } from "../src/client/db.ts";
import { ApiStatusSmokeView } from "../src/state-schema.ts";
import { storiesAddress } from "../src/server/stories.ts";

// Offline vertical smoke: fixture -> poller CAS append -> served State -> TanStack DB,
// including a departure during downtime and restart on the same SQLite file.

class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

/** A condition that has not converged yet; `eventually` retries it. */
class Pending extends Schema.TaggedError<Pending>()("Pending", { message: Schema.String }) {}

const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new SmokeFailure({ message }));

const eventually = <A, E, R>(attempt: Effect.Effect<A, E | Pending, R>, interval: Duration.Input) =>
  attempt.pipe(
    Effect.retry({
      while: (error) => Schema.is(Pending)(error),
      schedule: Schedule.spaced(interval).pipe(Schedule.upTo({ duration: "10 seconds" })),
    }),
    Effect.mapError((error) =>
      Schema.is(Pending)(error) ? new SmokeFailure({ message: error.message }) : error,
    ),
  );

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

const story = (id: number, time: number, title: string): FixtureStory => ({
  id,
  type: "story",
  by: `user-${id}`,
  time,
  title,
  score: id - 60,
  descendants: id - 95,
  url: `https://example.com/${id}`,
});

const baseTime = 1_700_000_000;
const fixtureById = new Map(
  [
    story(101, baseTime + 30, "Streamsy ships durable streams"),
    story(102, baseTime + 20, "Bounded projections over event streams"),
    story(103, baseTime + 40, "Durable State lineage"),
  ].map((value) => [value.id, value]),
);
let newestIds = [101, 102];

/** The HN API fixture: the newest id list and one item per id. */
const fixtureApp = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const pathname = new URL(request.url, "http://fixture").pathname;
  if (pathname === "/newstories.json") return HttpServerResponse.jsonUnsafe(newestIds);
  const itemMatch = pathname.match(/^\/item\/(\d+)\.json$/);
  if (itemMatch)
    return HttpServerResponse.jsonUnsafe(fixtureById.get(Number(itemMatch[1])) ?? null);
  return HttpServerResponse.text("not found", { status: 404 });
});

const StoryEvents = Schema.Array(
  StreamAddress.stateChange({
    type: "hn-story",
    schema: storiesAddress.collections["hn-story"].schema,
  }),
);

const smoke = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const cwd = path.resolve(import.meta.dir, "..");
  const scratchDir = yield* fs.makeTempDirectoryScoped({ prefix: "streamsy-hn-smoke-" });

  const fixture = yield* BunHttpServer.make({ port: 0 });
  yield* fixture.serve(fixtureApp);
  const fixturePort = new URL(HttpServer.formatAddress(fixture.address)).port;

  const serverOptions = {
    cwd,
    entry: "src/server/index.ts",
    env: {
      HN_API_BASE: `http://127.0.0.1:${fixturePort}`,
      HN_NEWEST_LIMIT: "2",
      HN_POLL_INTERVAL_MS: "600000",
      HN_DB: path.join(scratchDir, "hackernews.sqlite"),
    },
    isReady: (status: number) => status === 200,
  };
  const server = yield* spawnServer(serverOptions);
  const baseUrl = server.url;

  const waitForStatus = eventually(
    Effect.gen(function* () {
      const response = yield* HttpClient.get(`${baseUrl}/api/status`);
      if (response.status !== 200) {
        return yield* new Pending({ message: `HN status answered ${response.status}` });
      }
      const status = yield* HttpClientResponse.schemaBodyJson(ApiStatusSmokeView)(response);
      yield* check(!status.lastPollError, `poll reported an error: ${status.lastPollError}`);
      if (
        status.lastPollCompletedAt === undefined ||
        status.polling ||
        status.lastOffset === undefined
      ) {
        return yield* new Pending({
          message: `HN demo did not converge. Last status: ${Formatter.formatJson(status)}`,
        });
      }
      return status;
    }),
    "100 millis",
  );

  const readStoryEvents = Effect.gen(function* () {
    const response = yield* HttpClient.get(`${baseUrl}/state/newest?offset=-1`);
    yield* check(response.status === 200, `State stream read failed: ${response.status}`);
    yield* check(response.headers["x-streamsy-state-version"] === "1", "served state version");
    return yield* HttpClientResponse.schemaBodyJson(StoryEvents)(response);
  });

  const poll = Effect.gen(function* () {
    const response = yield* HttpClient.post(`${baseUrl}/api/poll`);
    yield* check(
      response.status >= 200 && response.status < 300,
      `poll failed: ${response.status}`,
    );
  });

  // Browser sessions close before the servers stop, as in a real page unload.
  const sessions: HnDb[] = [];
  const session = Effect.gen(function* () {
    const db = createHnDb(baseUrl);
    sessions.push(db);
    yield* Effect.tryPromise({
      try: () => db.preload(),
      catch: (cause) => new SmokeFailure({ message: `session preload failed: ${String(cause)}` }),
    });
    return db;
  });

  const waitForRows = (db: HnDb, title: string, ids = "101,103") =>
    eventually(
      Effect.suspend(() => {
        const rows = Array.from(db.collections.stories.values());
        const converged =
          rows.length === ids.split(",").length &&
          rows.some((row) => row.title === title) &&
          rows
            .map((row) => String(row.id))
            .toSorted()
            .join(",") === ids;
        return converged
          ? Effect.void
          : Effect.fail(
              new Pending({
                message: `Collection did not converge: ${Formatter.formatJson(rows)}`,
              }),
            );
      }),
      "20 millis",
    );

  const body = Effect.gen(function* () {
    const initialStatus = yield* waitForStatus;
    const initialEvents = yield* readStoryEvents;
    yield* check(initialStatus.lastUpserts === 2, "initial poll should append two upserts");
    yield* check(initialEvents.length === 2, "initial poll should emit two story upserts");

    const live = yield* session;
    yield* check(
      live.collections.stories.size === 2,
      "new session replays two current rows from -1",
    );
    const initialOffset = live.offset;
    const updated = yield* Effect.fromNullishOr(fixtureById.get(101)).pipe(
      Effect.mapError(() => new SmokeFailure({ message: "fixture story 101 should exist" })),
    );
    fixtureById.set(101, { ...updated, title: "Streamsy ships a complete projection", score: 43 });
    newestIds = [103, 101];
    yield* poll;
    const changedStatus = yield* waitForStatus;
    const changedEvents = yield* readStoryEvents;
    yield* waitForRows(live, "Streamsy ships a complete projection");
    yield* check(live.offset !== initialOffset, "same session consumes the live suffix after poll");
    const deleted = changedEvents.find((event) => event.headers.operation === "delete");
    yield* check(
      deleted !== undefined && "old_value" in deleted,
      "Durable State delete carries the departed row",
    );
    const replay = yield* session;
    yield* waitForRows(replay, "Streamsy ships a complete projection");

    yield* check(
      changedStatus.lastStoryCount === 2,
      "bounded newest set should still contain two rows",
    );
    yield* check(
      changedStatus.lastUpserts === 2 && changedStatus.lastDeletes === 1,
      "changed poll should append delete plus two upserts",
    );
    yield* check(
      changedEvents.some((event) => event.key === "102" && event.headers.operation === "delete"),
      "story leaving the newest set should emit a delete",
    );
    yield* check(
      changedEvents.some(
        (event) =>
          event.key === "101" &&
          event.headers.operation === "upsert" &&
          ("value" in event ? event.value.title : undefined) ===
            "Streamsy ships a complete projection",
      ),
      "mutable story fields should emit an updated upsert",
    );
    yield* check(
      changedEvents.some((event) => event.key === "103" && event.headers.operation === "upsert"),
      "story entering the newest set should emit an upsert",
    );

    yield* poll;
    const unchangedStatus = yield* waitForStatus;
    const unchangedEvents = yield* readStoryEvents;
    yield* check(
      unchangedStatus.lastUpserts === 0 && unchangedStatus.lastDeletes === 0,
      "unchanged poll must not append changes",
    );
    yield* check(
      unchangedEvents.length === changedEvents.length,
      "unchanged poll must not append State events",
    );

    const offsetBeforeRestart = unchangedStatus.lastOffset;
    // The stop fails unless the server exits 0 with empty stderr.
    yield* server.stop;
    // Story 103 leaves while the poller is stopped: F-1 must be repaired on the first pass.
    newestIds = [101];
    yield* spawnServer({ ...serverOptions, port: server.port });
    const restartedStatus = yield* waitForStatus;
    const restartedEvents = yield* readStoryEvents;
    yield* check(
      restartedStatus.lastOffset !== offsetBeforeRestart,
      "restart appends the departure",
    );
    yield* check(
      restartedStatus.lastUpserts === 0 && restartedStatus.lastDeletes === 1,
      "first restart poll deletes the downtime departure without redundant upserts",
    );
    yield* check(
      restartedEvents.length === unchangedEvents.length + 1,
      "restart appends exactly one delete",
    );
    yield* check(
      restartedEvents.at(-1)?.key === "103" &&
        restartedEvents.at(-1)?.headers.operation === "delete",
      "story 103 departed during downtime",
    );
    yield* check(
      Schema.toEquivalence(StoryEvents)(
        restartedEvents.slice(0, unchangedEvents.length),
        unchangedEvents,
      ),
      "restart preserves the retained prefix",
    );
    const restartedSession = yield* session;
    yield* waitForRows(restartedSession, "Streamsy ships a complete projection", "101");
    yield* waitForRows(live, "Streamsy ships a complete projection", "101");
    yield* eventually(
      Effect.suspend(() =>
        live.offset === restartedSession.offset
          ? Effect.void
          : Effect.fail(
              new Pending({ message: "pre-restart session resumes through downtime delete" }),
            ),
      ),
      "20 millis",
    );

    yield* poll;
    const restartedUnchangedStatus = yield* waitForStatus;
    const finalEvents = yield* readStoryEvents;
    yield* check(
      restartedUnchangedStatus.lastUpserts === 0 && restartedUnchangedStatus.lastDeletes === 0,
      "unchanged post-restart poll must not append changes",
    );
    yield* check(
      finalEvents.length === restartedEvents.length,
      "unchanged post-restart poll must be idle",
    );

    yield* Console.log(
      `hackernews-newest-stream HTTP smoke passed: ${finalEvents.length} State events; TanStack replay, live upsert/delete, and SQLite restart with downtime departure`,
    );
  });

  yield* body.pipe(Effect.ensuring(Effect.sync(() => sessions.forEach((db) => db.close()))));
});

smoke.pipe(
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
