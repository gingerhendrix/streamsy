import { BunRuntime, BunServices } from "@effect/platform-bun";
import { spawnServer } from "@streamsy/test-support/server";
import { Console, Effect, Fiber, FileSystem, Layer, Path, Result, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";
import { IssueTrackerApi } from "@streamsy/issue-tracker-demo-contract/api";
import {
  BadRequest,
  Conflict,
  NotFound,
  WipLimitReached,
} from "@streamsy/issue-tracker-demo-contract/api-schema";
import {
  decodeStateEvents,
  Issue,
  Project,
  type StateEvent,
} from "@streamsy/issue-tracker-demo-contract/state-schema";

class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

/** A read that does not hold the rows the smoke waits for yet. */
class NotYet extends Schema.TaggedError<NotYet>()("NotYet", { waitingFor: Schema.String }) {}

const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new SmokeFailure({ message }));

const apiErrors = { BadRequest, NotFound, Conflict, WipLimitReached };

/** Run a call that must fail with the API error `tag`; return its `error` message. */
const expectApiError = <A, E, R>(effect: Effect.Effect<A, E, R>, tag: keyof typeof apiErrors) =>
  Effect.gen(function* () {
    const failure = yield* Effect.flip(effect);

    if (!Schema.is(apiErrors[tag])(failure)) {
      return yield* new SmokeFailure({ message: `expected ${tag}, got ${String(failure)}` });
    }

    return failure.error;
  });

/** POST a raw (possibly non-JSON) body and return only the status. */
const postRawStatus = (url: string, body: string) =>
  Effect.map(
    HttpClient.post(url, { body: HttpBody.text(body, "application/json") }),
    (response) => response.status,
  );

const readStream = (streamUrl: string) =>
  Effect.gen(function* () {
    const response = yield* HttpClient.get(`${streamUrl}?offset=-1`);
    yield* check(response.status === 200, `stream read status ${response.status}`);
    yield* check(
      response.headers["stream-up-to-date"] === "true",
      "stream read should be up to date",
    );
    yield* check(
      response.headers["x-streamsy-state-version"] === "1",
      "the stream route should serve the Durable State contract",
    );
    const head = response.headers["stream-next-offset"];

    if (head === undefined || head === "") {
      return yield* new SmokeFailure({
        message: "stream read should return a stream-next-offset header",
      });
    }

    const events = yield* decodeStateEvents(yield* response.json).pipe(
      Effect.fromResult,
      Effect.mapError(
        () => new SmokeFailure({ message: "stream read body should hold change events" }),
      ),
    );

    return { events: [...events], head };
  });

type StreamRead = Effect.Success<ReturnType<typeof readStream>>;

/**
 * Read until `ready` holds, for up to five seconds. The base app writes the
 * State stream in the request, so the first read holds the change. A variant
 * that projects the stream from events can answer before the row lands, as a
 * browser that waits on the txid allows.
 */
const readUntil = (streamUrl: string, waitingFor: string, ready: (read: StreamRead) => boolean) =>
  readStream(streamUrl).pipe(
    Effect.filterOrFail(ready, () => new NotYet({ waitingFor })),
    Effect.retry({ while: Schema.is(NotYet), schedule: Schedule.spaced("50 millis"), times: 100 }),
    Effect.catchTag(
      "NotYet",
      () => new SmokeFailure({ message: `timed out waiting for ${waitingFor}` }),
    ),
  );

/** The read holds a change with each txid. */
const hasTxids =
  (...txids: ReadonlyArray<string>) =>
  ({ events }: StreamRead) =>
    txids.every((txid) => events.some((event) => event.headers.txid === txid));

const findEvent = (events: StateEvent[], type: string, key: string, operation: string) => {
  const match = events.find(
    (event) => event.type === type && event.key === key && event.headers.operation === operation,
  );

  return match === undefined
    ? Effect.fail(
        new SmokeFailure({
          message: `expected a ${operation} ${type} event for ${key} in the stream`,
        }),
      )
    : Effect.succeed(match);
};

const smoke = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // The app folder to start. The default is this demo. A variant passes its own
  // folder, so one smoke checks every server against the same wire contract.
  const cwd = path.resolve(process.argv[2] ?? path.resolve(import.meta.dir, ".."));
  const app = path.basename(cwd);
  const tempDirectory = yield* fs.makeTempDirectoryScoped({ prefix: "streamsy-issue-tracker-" });
  const databasePath = path.join(tempDirectory, "smoke.sqlite");

  const serverOptions = {
    cwd,
    entry: "server/index.ts",
    env: { NODE_ENV: "production", ISSUE_TRACKER_DB: databasePath },
    isReady: (status: number) => status === 200,
  };

  const server = yield* spawnServer(serverOptions);
  const baseUrl = server.url;
  const mainStreamUrl = `${baseUrl}/streams/workspace/main`;
  /** The typed client for the same IssueTrackerApi the server implements. */
  const api = yield* HttpApiClient.make(IssueTrackerApi, { baseUrl });

  const main = { ws: "main" };

  // === 0. The seed: five issues, two of them in progress in one project ===

  const seed = yield* readUntil(
    mainStreamUrl,
    "the seeded issues",
    ({ events }) => events.filter((event) => event.type === "issue").length >= 5,
  );

  const seededIssues = yield* Effect.forEach(
    seed.events.filter((event) => event.type === "issue"),
    (event) => Schema.decodeUnknownEffect(Issue)(event.value),
  );

  yield* check(seededIssues.length === 5, `main should seed 5 issues, got ${seededIssues.length}`);
  yield* check(
    seededIssues.filter(
      (issue) => issue.projectId === "proj_streamsy" && issue.status === "in_progress",
    ).length === 2,
    "main should seed 2 issues in progress in Streamsy Demo",
  );

  // === 1. CRUD flow on the known workspace (/api/w/main) ===

  // 1a. Create a project.
  const projectResult = yield* api.projects.create({
    params: main,
    payload: { name: "Smoke Project", description: "created by http-smoke" },
  });

  const projectId = projectResult.project.id;
  yield* check(projectResult.txid !== "", "create project should return a txid");
  yield* check(projectResult.awaitOffset !== "", "create project should return an awaitOffset");

  // 1b. Reject an issue with an unknown projectId (validation path).
  const unknownProject = yield* expectApiError(
    api.issues.create({
      params: main,
      payload: { projectId: "proj_does_not_exist", title: "nope" },
    }),
    "BadRequest",
  );

  yield* check(
    unknownProject === "Unknown projectId",
    `unknown projectId message: ${unknownProject}`,
  );

  // 1c. Create an issue under the project.
  const issueResult = yield* api.issues.create({
    params: main,
    payload: { projectId, title: "Smoke issue" },
  });

  const issueId = issueResult.issue.id;
  yield* check(issueResult.issue.status === "open", "new issue should default to open");

  // 1d. Update the issue status (create/update flow).
  const updateResult = yield* api.issues.update({
    params: { ...main, id: issueId },
    payload: { status: "done" },
  });

  yield* check(updateResult.issue.status === "done", "issue update should set status to done");

  // 1e. Comment on the issue.
  const commentResult = yield* api.comments.create({
    params: main,
    payload: { issueId, body: "looks good" },
  });

  const commentId = commentResult.comment.id;

  // 1f. Read the durable stream and verify every mutation produced a
  //     client-readable Durable State change event.
  const { events } = yield* readUntil(
    mainStreamUrl,
    "the CRUD changes",
    hasTxids(commentResult.txid),
  );

  const projectEvent = yield* findEvent(events, "project", projectId, "upsert");
  const projectValue = yield* Schema.decodeUnknownEffect(Project)(projectEvent.value);
  yield* check(projectValue.id === projectId, "project event value should carry the project id");

  yield* findEvent(events, "issue", issueId, "upsert");

  const issueUpdate = events.findLast(
    (event) =>
      event.type === "issue" && event.key === issueId && event.headers.operation === "upsert",
  );

  yield* check(issueUpdate !== undefined, `expected the updated issue ${issueId} as an upsert`);
  const updatedIssue = yield* Schema.decodeUnknownEffect(Issue)(issueUpdate?.value);
  yield* check(updatedIssue.status === "done", "issue update event should carry status=done");

  const commentEvent = yield* findEvent(events, "comment", commentId, "upsert");
  yield* check(
    commentEvent.type === "comment" && commentEvent.value?.issueId === issueId,
    "comment event value should reference its issue",
  );

  // === 2. Shareable workspace creation ===

  const createWorkspace = Effect.gen(function* () {
    const { id } = yield* api.workspaces.create();
    yield* check(/^[a-z0-9]{10}$/.test(id), `workspace id should be 10 base36 chars: ${id}`);

    return id;
  });

  const workspaceId = yield* createWorkspace;
  const secondWorkspaceId = yield* createWorkspace;
  yield* check(workspaceId !== secondWorkspaceId, "each created workspace should get a fresh id");

  // === 3. New workspace stream contains exactly the starter-project upsert ===

  const workspaceStreamUrl = `${baseUrl}/streams/workspace/${workspaceId}`;

  const starterRead = yield* readUntil(
    workspaceStreamUrl,
    "the starter project",
    (read) => read.events.length > 0,
  );

  yield* check(
    starterRead.events.length === 1,
    `new workspace should hold exactly 1 seed event, got ${starterRead.events.length}`,
  );
  const starter = starterRead.events[0];
  yield* check(starter?.type === "project", "starter event should be a project event");
  yield* check(starter?.headers.operation === "upsert", "starter event should be an upsert");
  const starterProject = yield* Schema.decodeUnknownEffect(Project)(starter?.value);
  yield* check(
    starterProject.name === "Getting started",
    "starter project should be 'Getting started'",
  );
  const starterProjectId = starterProject.id;

  // === 4. Isolation between workspaces ===

  // The project created in main must not appear in the new workspace.
  const isolationRead = yield* readStream(workspaceStreamUrl);
  yield* check(
    !isolationRead.events.some((event) => event.key === projectId),
    "project created in main must not appear in the shared workspace stream",
  );

  // Validation runs against the workspace's own materialized state: an issue
  // in the new workspace referencing a main project id must 400.
  yield* expectApiError(
    api.issues.create({
      params: { ws: workspaceId },
      payload: { projectId, title: "cross-workspace reference" },
    }),
    "BadRequest",
  );

  // And the new workspace accepts issues against its own starter project.
  yield* api.issues.create({
    params: { ws: workspaceId },
    payload: { projectId: starterProjectId, title: "First shared issue" },
  });

  // === 5. Unknown and malformed requests ===

  // The typed client cannot send these bodies, so they stay raw requests.
  const projectsUrl = `${baseUrl}/api/w/main/projects`;
  const invalidJsonStatus = yield* postRawStatus(projectsUrl, "{ not json");
  yield* check(invalidJsonStatus === 400, `invalid JSON body should 400, got ${invalidJsonStatus}`);

  const arrayBodyStatus = yield* postRawStatus(projectsUrl, "[]");
  yield* check(
    arrayBodyStatus === 400,
    `a non-object JSON body should 400, got ${arrayBodyStatus}`,
  );

  const badTxidStatus = yield* postRawStatus(projectsUrl, `{"name":"x","txid":"nope"}`);
  yield* check(badTxidStatus === 400, `a malformed txid should 400, got ${badTxidStatus}`);

  const badFieldResponse = yield* HttpClient.post(projectsUrl, {
    body: HttpBody.text(`{"name":42}`, "application/json"),
  });

  yield* check(
    badFieldResponse.status === 400,
    `a non-string name should 400, got ${badFieldResponse.status}`,
  );
  const badField = yield* badFieldResponse.json;
  yield* check(
    Schema.is(Schema.Struct({ error: Schema.String }))(badField) && badField.error.includes("name"),
    "a field error should name the field in its error message",
  );

  const badStatusResponse = yield* HttpClient.post(`${baseUrl}/api/w/main/issues`, {
    body: HttpBody.jsonUnsafe({ projectId, title: "bad status", status: "archived" }),
  });

  const badStatusStatus = badStatusResponse.status;
  yield* check(
    badStatusStatus === 400,
    `a status outside the schema should 400, got ${badStatusStatus}`,
  );

  const malformedStatus = yield* postRawStatus(`${baseUrl}/api/w/-bad/projects`, `{"name":"x"}`);
  yield* check(
    malformedStatus === 400,
    `malformed workspace id should 400, got ${malformedStatus}`,
  );

  yield* expectApiError(
    api.projects.create({ params: { ws: "nope12345" }, payload: { name: "x" } }),
    "NotFound",
  );

  const unknownApi = yield* HttpClient.get(`${baseUrl}/api/w/main/unknown`);
  yield* check(unknownApi.status === 404, `unknown /api path should 404, got ${unknownApi.status}`);
  yield* check(
    Schema.is(Schema.Struct({ error: Schema.String }))(yield* unknownApi.json),
    "unknown /api path should answer a JSON error",
  );

  // === 6. Contention: parallel writes converge via CAS retry ===

  const before = yield* readStream(mainStreamUrl);
  const contentionCount = 10;

  const contentionResults = yield* Effect.all(
    Array.from({ length: contentionCount }, (_, index) =>
      api.issues.create({
        params: main,
        payload: { projectId, title: `Contention issue ${index + 1}` },
      }),
    ),
    { concurrency: "unbounded" },
  );

  const contentionIds = contentionResults.map((result) => result.issue.id);
  yield* check(
    new Set(contentionIds).size === contentionCount,
    "contended issue creates should produce unique issues",
  );

  const after = yield* readUntil(
    mainStreamUrl,
    "the contended issues",
    hasTxids(...contentionResults.map((result) => result.txid)),
  );

  yield* check(
    after.events.length === before.events.length + contentionCount,
    `stream should gain exactly ${contentionCount} events under contention ` +
      `(got ${after.events.length - before.events.length}) — no lost or duplicate appends`,
  );

  for (const contendedId of contentionIds) {
    yield* findEvent(after.events, "issue", contendedId, "upsert");
  }

  // === 7. Two writers race for the last in-progress place ===

  const raceWorkspaceId = yield* createWorkspace;
  const race = { ws: raceWorkspaceId };

  const raceProject = yield* Schema.decodeUnknownEffect(Project)(
    (yield* readUntil(
      `${baseUrl}/streams/workspace/${raceWorkspaceId}`,
      "the race starter project",
      (read) => read.events.length > 0,
    )).events[0]?.value,
  );

  const raceIssue = (title: string, status: "open" | "in_progress") =>
    Effect.map(
      api.issues.create({ params: race, payload: { projectId: raceProject.id, title, status } }),
      (result) => result.issue.id,
    );

  yield* raceIssue("Busy 1", "in_progress");
  yield* raceIssue("Busy 2", "in_progress");

  const contenders = [
    yield* raceIssue("Contender A", "open"),
    yield* raceIssue("Contender B", "open"),
  ];

  const raced = yield* Effect.forEach(
    contenders,
    (id) =>
      api.issues
        .update({ params: { ...race, id }, payload: { status: "in_progress" } })
        .pipe(Effect.result),
    { concurrency: "unbounded" },
  );

  const refused = raced.filter(Result.isFailure).map((result) => result.failure);
  yield* check(
    raced.filter(Result.isSuccess).length === 1 &&
      refused.length === 1 &&
      Schema.is(WipLimitReached)(refused[0]),
    `the race should give one success and one WipLimitReached, got ${refused.map(String).join(", ")}`,
  );

  // === 8. Raw stream writes cannot bypass domain validation ===

  const beforeDenied = yield* readStream(mainStreamUrl);

  for (const method of ["PUT", "POST", "DELETE"] as const) {
    const denied = yield* HttpClient.execute(HttpClientRequest.make(method)(mainStreamUrl));
    yield* check(denied.status === 405, `raw ${method} should 405, got ${denied.status}`);
  }

  const unknownStream = yield* HttpClient.get(`${baseUrl}/streams/private/unknown`);
  yield* check(unknownStream.status === 404, "undeclared stream should 404");
  yield* check(
    (yield* readStream(mainStreamUrl)).head === beforeDenied.head,
    "denied writes leave the stream unchanged",
  );

  // === 9. SQLite restart preserves a workspace and its issue ===

  const restartWorkspaceId = yield* createWorkspace;
  const restartStreamUrl = `${baseUrl}/streams/workspace/${restartWorkspaceId}`;

  const beforeRestartIssue = yield* readUntil(
    restartStreamUrl,
    "the restart starter project",
    (read) => read.events.length > 0,
  );

  const restartProject = yield* Schema.decodeUnknownEffect(Project)(
    beforeRestartIssue.events[0]?.value,
  );

  const restartIssue = yield* api.issues.create({
    params: { ws: restartWorkspaceId },
    payload: { projectId: restartProject.id, title: "Survives restart" },
  });

  const restartIssueId = restartIssue.issue.id;

  const { head: restartHead } = yield* readUntil(
    restartStreamUrl,
    "the issue before the restart",
    hasTxids(restartIssue.txid),
  );

  // A browser normally has a parked read when the server stops.
  // Serve.layer answers it like a long-poll timeout (204 at the same head),
  // then the listener stops and SQLite closes.
  const parked = yield* Effect.forkScoped(
    HttpClient.get(`${restartStreamUrl}?offset=now&live=long-poll`),
  );

  yield* Effect.sleep("250 millis");
  yield* check(parked.pollUnsafe() === undefined, "shutdown probe should have a parked long poll");
  // The stop fails unless the server exits 0 with empty stderr.
  yield* server.stop;
  const drained = yield* Fiber.join(parked);
  yield* check(drained.status === 204, `shutdown long-poll status ${drained.status}`);
  yield* check(
    drained.headers["stream-next-offset"] === restartHead,
    "the drained long poll should answer at the workspace head",
  );
  yield* check(!(yield* fs.exists(`${databasePath}-wal`)), "shutdown should close SQLite WAL");
  yield* check(!(yield* fs.exists(`${databasePath}-shm`)), "shutdown should close SQLite SHM");

  yield* spawnServer({ ...serverOptions, port: server.port });

  const afterRestart = yield* readStream(restartStreamUrl);
  yield* findEvent(afterRestart.events, "issue", restartIssueId, "upsert");
  yield* check(
    afterRestart.head === restartHead,
    "workspace head after restart should match the persisted issue append",
  );

  // The restarted server takes writes, and they reach the stream.
  const afterRestartIssue = yield* api.issues.create({
    params: { ws: restartWorkspaceId },
    payload: { projectId: restartProject.id, title: "Written after restart" },
  });

  yield* readUntil(
    restartStreamUrl,
    "the issue after the restart",
    hasTxids(afterRestartIssue.txid),
  );

  yield* Console.log(
    `${app} HTTP smoke passed: main project ${projectId}, issue ${issueId}, ` +
      `shared workspace ${workspaceId}, ${after.events.length} main stream events`,
  );
});

smoke.pipe(
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
