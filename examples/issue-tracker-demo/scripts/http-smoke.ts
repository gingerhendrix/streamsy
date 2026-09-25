import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Effect, Result, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { HttpApiClient } from "effect/unstable/httpapi";
import { IssueTrackerApi } from "../shared/api.ts";
import { BadRequest, Conflict, NotFound } from "../shared/api-schema.ts";
import { decodeStateEvents, Issue, Project, type StateEvent } from "../shared/state-schema.ts";

const packageDir = resolve(import.meta.dir, "..");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;
const mainStreamUrl = `${baseUrl}/streams/workspace/main`;
const tempDirectory = await mkdtemp(join(tmpdir(), "streamsy-issue-tracker-"));
const databasePath = join(tempDirectory, "smoke.sqlite");

class SmokeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SmokeError";
  }
}

type AssertionCondition = boolean | string | StateEvent | null | undefined;

function assert(condition: AssertionCondition, message: string): asserts condition {
  if (!condition) {
    throw new SmokeError(message);
  }
}

/** The typed client for the same IssueTrackerApi the server implements. */
const api = await Effect.runPromise(
  HttpApiClient.make(IssueTrackerApi, { baseUrl }).pipe(Effect.provide(FetchHttpClient.layer)),
);

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(effect);

const apiErrors = { BadRequest, NotFound, Conflict };

/** Run a call that must fail with the API error `tag`; return its `error` message. */
async function expectApiError<A, E>(
  effect: Effect.Effect<A, E>,
  tag: keyof typeof apiErrors,
): Promise<string> {
  const failure = await run(Effect.flip(effect));
  assert(Schema.is(apiErrors[tag])(failure), `expected ${tag}, got ${String(failure)}`);
  return failure.error;
}

/** POST a raw (possibly non-JSON) body and return only the status. */
async function postRawStatus(path: string, body: string): Promise<number> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
  await response.text();
  return response.status;
}

async function waitForServer(): Promise<void> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(baseUrl);
      if (response.status === 200) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(100);
  }

  throw new SmokeError(`Issue tracker server did not become ready: ${String(lastError)}`);
}

async function readStream(streamUrl: string): Promise<{ events: StateEvent[]; head: string }> {
  const response = await fetch(`${streamUrl}?offset=-1`);
  assert(response.status === 200, `stream read status ${response.status}`);
  assert(response.headers.get("stream-up-to-date") === "true", "stream read should be up to date");
  const head = response.headers.get("stream-next-offset");
  assert(head, "stream read should return a stream-next-offset header");
  const events = decodeStateEvents(await response.json());
  assert(Result.isSuccess(events), "stream read body should hold change events");
  return { events: [...events.success], head };
}

function findEvent(events: StateEvent[], type: string, key: string, operation: string): StateEvent {
  const match = events.find(
    (event) => event.type === type && event.key === key && event.headers.operation === operation,
  );
  assert(match !== undefined, `expected a ${operation} ${type} event for ${key} in the stream`);
  return match;
}

const startServer = () =>
  Bun.spawn(["bun", "server/index.ts"], {
    cwd: packageDir,
    env: {
      ...process.env,
      NODE_ENV: "production",
      PORT: String(port),
      ISSUE_TRACKER_DB: databasePath,
    },
    stdout: "pipe",
    stderr: "pipe",
  });

let server = startServer();

try {
  await waitForServer();

  const main = { ws: "main" };

  // === 1. CRUD flow on the known workspace (/api/w/main) ===

  // 1a. Create a project.
  const projectResult = await run(
    api.projects.create({
      params: main,
      payload: { name: "Smoke Project", description: "created by http-smoke" },
    }),
  );
  const projectId = projectResult.project.id;
  assert(projectResult.txid, "create project should return a txid");
  assert(projectResult.awaitOffset, "create project should return an awaitOffset");

  // 1b. Reject an issue with an unknown projectId (validation path).
  const unknownProject = await expectApiError(
    api.issues.create({
      params: main,
      payload: { projectId: "proj_does_not_exist", title: "nope" },
    }),
    "BadRequest",
  );
  assert(unknownProject === "Unknown projectId", `unknown projectId message: ${unknownProject}`);

  // 1c. Create an issue under the project.
  const issueResult = await run(
    api.issues.create({ params: main, payload: { projectId, title: "Smoke issue" } }),
  );
  const issueId = issueResult.issue.id;
  assert(issueResult.issue.status === "open", "new issue should default to open");

  // 1d. Update the issue status (create/update flow).
  const updateResult = await run(
    api.issues.update({ params: { ...main, id: issueId }, payload: { status: "done" } }),
  );
  assert(updateResult.issue.status === "done", "issue update should set status to done");

  // 1e. Comment on the issue.
  const commentResult = await run(
    api.comments.create({ params: main, payload: { issueId, body: "looks good" } }),
  );
  const commentId = commentResult.comment.id;

  // 1f. Read the durable stream and verify every mutation produced a
  //     client-readable Durable State change event.
  const { events } = await readStream(mainStreamUrl);

  const projectEvent = findEvent(events, "project", projectId, "upsert");
  const projectValue = Schema.decodeUnknownSync(Project)(projectEvent.value);
  assert(projectValue.id === projectId, "project event value should carry the project id");

  findEvent(events, "issue", issueId, "upsert");

  const issueUpdate = events.findLast(
    (event) =>
      event.type === "issue" && event.key === issueId && event.headers.operation === "upsert",
  );
  assert(issueUpdate, `expected the updated issue ${issueId} as an upsert`);
  const updatedIssue = Schema.decodeUnknownSync(Issue)(issueUpdate.value);
  assert(updatedIssue.status === "done", "issue update event should carry status=done");

  const commentEvent = findEvent(events, "comment", commentId, "upsert");
  assert(
    commentEvent.type === "comment" && commentEvent.value?.issueId === issueId,
    "comment event value should reference its issue",
  );

  // === 2. Shareable workspace creation ===

  const createWorkspace = async (): Promise<string> => {
    const { id } = await run(api.workspaces.create());
    assert(/^[a-z0-9]{10}$/.test(id), `workspace id should be 10 base36 chars: ${id}`);
    return id;
  };

  const workspaceId = await createWorkspace();
  const secondWorkspaceId = await createWorkspace();
  assert(workspaceId !== secondWorkspaceId, "each created workspace should get a fresh id");

  // === 3. New workspace stream contains exactly the starter-project upsert ===

  const workspaceStreamUrl = `${baseUrl}/streams/workspace/${workspaceId}`;
  const starterRead = await readStream(workspaceStreamUrl);
  assert(
    starterRead.events.length === 1,
    `new workspace should hold exactly 1 seed event, got ${starterRead.events.length}`,
  );
  const starter = starterRead.events[0];
  assert(starter.type === "project", "starter event should be a project event");
  assert(starter.headers.operation === "upsert", "starter event should be an upsert");
  const starterProject = Schema.decodeUnknownSync(Project)(starter.value);
  assert(starterProject.name === "Getting started", "starter project should be 'Getting started'");
  const starterProjectId = starterProject.id;

  // === 4. Isolation between workspaces ===

  // The project created in main must not appear in the new workspace.
  const isolationRead = await readStream(workspaceStreamUrl);
  assert(
    !isolationRead.events.some((event) => event.key === projectId),
    "project created in main must not appear in the shared workspace stream",
  );

  // Validation runs against the workspace's own materialized state: an issue
  // in the new workspace referencing a main project id must 400.
  await expectApiError(
    api.issues.create({
      params: { ws: workspaceId },
      payload: { projectId, title: "cross-workspace reference" },
    }),
    "BadRequest",
  );

  // And the new workspace accepts issues against its own starter project.
  await run(
    api.issues.create({
      params: { ws: workspaceId },
      payload: { projectId: starterProjectId, title: "First shared issue" },
    }),
  );

  // === 5. Unknown and malformed requests ===

  // The typed client cannot send these bodies, so they stay raw fetches.
  const invalidJsonStatus = await postRawStatus("/api/w/main/projects", "{ not json");
  assert(invalidJsonStatus === 400, `invalid JSON body should 400, got ${invalidJsonStatus}`);

  const arrayBodyStatus = await postRawStatus("/api/w/main/projects", "[]");
  assert(arrayBodyStatus === 400, `a non-object JSON body should 400, got ${arrayBodyStatus}`);

  const badTxidStatus = await postRawStatus("/api/w/main/projects", `{"name":"x","txid":"nope"}`);
  assert(badTxidStatus === 400, `a malformed txid should 400, got ${badTxidStatus}`);

  const badFieldResponse = await fetch(`${baseUrl}/api/w/main/projects`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: `{"name":42}`,
  });
  assert(
    badFieldResponse.status === 400,
    `a non-string name should 400, got ${badFieldResponse.status}`,
  );
  const badField: unknown = await badFieldResponse.json();
  assert(
    Schema.is(Schema.Struct({ error: Schema.String }))(badField) && badField.error.includes("name"),
    "a field error should name the field in its error message",
  );

  const badStatusStatus = await postRawStatus(
    "/api/w/main/issues",
    JSON.stringify({ projectId, title: "bad status", status: "archived" }),
  );
  assert(badStatusStatus === 400, `a status outside the schema should 400, got ${badStatusStatus}`);

  const malformedStatus = await postRawStatus("/api/w/-bad/projects", `{"name":"x"}`);
  assert(malformedStatus === 400, `malformed workspace id should 400, got ${malformedStatus}`);

  await expectApiError(
    api.projects.create({ params: { ws: "nope12345" }, payload: { name: "x" } }),
    "NotFound",
  );

  const unknownApi = await fetch(`${baseUrl}/api/w/main/unknown`);
  assert(unknownApi.status === 404, `unknown /api path should 404, got ${unknownApi.status}`);
  assert(
    Schema.is(Schema.Struct({ error: Schema.String }))(await unknownApi.json()),
    "unknown /api path should answer a JSON error",
  );

  // === 6. Contention: parallel writes converge via CAS retry ===

  const before = await readStream(mainStreamUrl);
  const contentionCount = 10;
  const contentionResults = await run(
    Effect.all(
      Array.from({ length: contentionCount }, (_, index) =>
        api.issues.create({
          params: main,
          payload: { projectId, title: `Contention issue ${index + 1}` },
        }),
      ),
      { concurrency: "unbounded" },
    ),
  );
  const contentionIds = contentionResults.map((result) => result.issue.id);
  assert(
    new Set(contentionIds).size === contentionCount,
    "contended issue creates should produce unique issues",
  );
  const after = await readStream(mainStreamUrl);
  assert(
    after.events.length === before.events.length + contentionCount,
    `stream should gain exactly ${contentionCount} events under contention ` +
      `(got ${after.events.length - before.events.length}) — no lost or duplicate appends`,
  );
  for (const contendedId of contentionIds) {
    findEvent(after.events, "issue", contendedId, "upsert");
  }

  // === 7. Raw stream writes cannot bypass domain validation ===

  const beforeDenied = await readStream(mainStreamUrl);
  for (const method of ["PUT", "POST", "DELETE"]) {
    const denied = await fetch(mainStreamUrl, { method });
    assert(denied.status === 405, `raw ${method} should 405, got ${denied.status}`);
    await denied.text();
  }
  const unknownStream = await fetch(`${baseUrl}/streams/private/unknown`);
  assert(unknownStream.status === 404, "undeclared stream should 404");
  assert(
    (await readStream(mainStreamUrl)).head === beforeDenied.head,
    "denied writes leave the stream unchanged",
  );

  // === 8. SQLite restart preserves a workspace and its issue ===

  const restartWorkspaceId = await createWorkspace();
  const restartStreamUrl = `${baseUrl}/streams/workspace/${restartWorkspaceId}`;
  const beforeRestartIssue = await readStream(restartStreamUrl);
  const restartProject = Schema.decodeUnknownSync(Project)(beforeRestartIssue.events[0]?.value);
  const restartIssue = await run(
    api.issues.create({
      params: { ws: restartWorkspaceId },
      payload: { projectId: restartProject.id, title: "Survives restart" },
    }),
  );
  const restartIssueId = restartIssue.issue.id;

  // A browser normally has a parked read when the server stops. The layer
  // graph interrupts it (503), stops the listener, then closes SQLite.
  let settled = false;
  const parked = fetch(`${restartStreamUrl}?offset=now&live=long-poll`).then((response) => {
    settled = true;
    return response;
  });
  await Bun.sleep(250);
  assert(!settled, "shutdown probe should have a parked long poll");
  server.kill();
  // BunRuntime.runMain reports an interrupted main fiber with exit code 130.
  const exitCode = await server.exited;
  assert(exitCode === 130, `parked-read shutdown should exit 130 (interrupted), got ${exitCode}`);
  const interrupted = await parked;
  assert(interrupted.status === 503, `shutdown long-poll status ${interrupted.status}`);
  await interrupted.text();
  assert(!(await Bun.file(`${databasePath}-wal`).exists()), "shutdown should close SQLite WAL");
  assert(!(await Bun.file(`${databasePath}-shm`).exists()), "shutdown should close SQLite SHM");
  const stoppedStderr = await new Response(server.stderr).text();
  server = startServer();
  assert(stoppedStderr.trim() === "", `shutdown wrote to stderr: ${stoppedStderr}`);
  await waitForServer();

  const afterRestart = await readStream(restartStreamUrl);
  findEvent(afterRestart.events, "issue", restartIssueId, "upsert");
  assert(
    afterRestart.head === restartIssue.awaitOffset,
    "workspace head after restart should match the persisted issue append",
  );

  console.log(
    `issue-tracker-demo HTTP smoke passed: main project ${projectId}, issue ${issueId}, ` +
      `shared workspace ${workspaceId}, ${after.events.length} main stream events`,
  );
} finally {
  server.kill();
  await server.exited.catch(() => undefined);

  const stdout = await new Response(server.stdout).text();
  const stderr = await new Response(server.stderr).text();
  if (stdout.trim()) {
    console.log(stdout.trim());
  }
  if (stderr.trim()) {
    console.error(stderr.trim());
  }
  await rm(tempDirectory, { recursive: true, force: true });
}
