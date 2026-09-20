import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  commentSchema,
  issueSchema,
  mutationResultSchema,
  projectSchema,
  stateEventsSchema,
  workspaceResultSchema,
  type MutationResult,
  type JsonValue,
  type StateEvent,
} from "../shared/state-schema.ts";

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

function probeEvent(suffix: string) {
  return {
    type: "project",
    key: `proj_probe_${suffix}`,
    value: {
      id: `proj_probe_${suffix}`,
      name: `Probe ${suffix}`,
      description: "expectedOffset probe",
      createdAt: new Date().toISOString(),
    },
    headers: {
      operation: "upsert",
      txid: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
    },
  };
}

async function postJson(
  path: string,
  body: JsonValue,
  method: "POST" | "PATCH" = "POST",
): Promise<MutationResult> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  assert(response.ok, `${method} ${path} expected 2xx, got ${response.status}: ${text}`);
  const payload = mutationResultSchema.safeParse(JSON.parse(text));
  assert(payload.success, `${method} ${path} should return awaitOffset and txid: ${text}`);
  return payload.data;
}

async function postStatus(path: string, body: JsonValue): Promise<number> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  await response.text();
  return response.status;
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
  const events = stateEventsSchema.safeParse(await response.json());
  assert(events.success, "stream read body should hold change events");
  return { events: events.data, head };
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
    env: { ...process.env, PORT: String(port), ISSUE_TRACKER_DB: databasePath },
    stdout: "pipe",
    stderr: "pipe",
  });

let server = startServer();

try {
  await waitForServer();

  // === 1. CRUD flow on the known workspace (/api/w/main) ===

  // 1a. Create a project.
  const projectResult = await postJson("/api/w/main/projects", {
    name: "Smoke Project",
    description: "created by http-smoke",
  });
  const projectId = projectResult.project?.id;
  assert(projectId, "create project should return a project id");
  assert(projectResult.txid, "create project should return a txid");
  assert(projectResult.awaitOffset, "create project should return an awaitOffset");

  // 1b. Reject an issue with an unknown projectId (validation path).
  const badIssueStatus = await postStatus("/api/w/main/issues", {
    projectId: "proj_does_not_exist",
    title: "nope",
  });
  assert(badIssueStatus === 400, `unknown projectId should 400, got ${badIssueStatus}`);

  // 1c. Create an issue under the project.
  const issueResult = await postJson("/api/w/main/issues", {
    projectId,
    title: "Smoke issue",
  });
  const issueId = issueResult.issue?.id;
  assert(issueId, "create issue should return an issue id");
  assert(issueResult.issue?.status === "open", "new issue should default to open");

  // 1d. Update the issue status (create/update flow).
  const updateResult = await postJson(
    `/api/w/main/issues/${encodeURIComponent(issueId)}`,
    { status: "done" },
    "PATCH",
  );
  assert(updateResult.issue?.status === "done", "issue update should set status to done");

  // 1e. Comment on the issue.
  const commentResult = await postJson("/api/w/main/comments", {
    issueId,
    body: "looks good",
  });
  const commentId = commentResult.comment?.id;
  assert(commentId, "create comment should return a comment id");

  // 1f. Read the durable stream and verify every mutation produced a
  //     client-readable Durable State change event.
  const { events } = await readStream(mainStreamUrl);

  const projectEvent = findEvent(events, "project", projectId, "upsert");
  const projectValue = projectSchema.parse(projectEvent.value);
  assert(projectValue.id === projectId, "project event value should carry the project id");

  findEvent(events, "issue", issueId, "upsert");

  const issueUpdate = events.findLast(
    (event) =>
      event.type === "issue" && event.key === issueId && event.headers.operation === "upsert",
  );
  assert(issueUpdate, `expected the updated issue ${issueId} as an upsert`);
  const updatedIssue = issueSchema.parse(issueUpdate.value);
  assert(updatedIssue.status === "done", "issue update event should carry status=done");

  const commentEvent = findEvent(events, "comment", commentId, "upsert");
  const commentValue = commentSchema.parse(commentEvent.value);
  assert(commentValue.issueId === issueId, "comment event value should reference its issue");

  // === 2. Shareable workspace creation ===

  const createWorkspace = async (): Promise<string> => {
    const response = await fetch(`${baseUrl}/api/workspaces`, { method: "POST" });
    assert(response.status === 201, `POST /api/workspaces expected 201, got ${response.status}`);
    const body = workspaceResultSchema.parse(await response.json());
    assert(/^[a-z0-9]{10}$/.test(body.id), `workspace id should be 10 base36 chars: ${body.id}`);
    return body.id;
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
  const starterProject = projectSchema.parse(starter.value);
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
  const crossWorkspaceStatus = await postStatus(`/api/w/${workspaceId}/issues`, {
    projectId,
    title: "cross-workspace reference",
  });
  assert(
    crossWorkspaceStatus === 400,
    `issue referencing a main project in a shared workspace should 400, got ${crossWorkspaceStatus}`,
  );

  // And the new workspace accepts issues against its own starter project.
  const sharedIssue = await postJson(`/api/w/${workspaceId}/issues`, {
    projectId: starterProjectId,
    title: "First shared issue",
  });
  assert(sharedIssue.issue?.id, "shared workspace issue create should return an issue id");

  // === 5. Unknown and malformed workspace ids ===

  const invalidJsonStatus = await postRawStatus("/api/w/main/projects", "{ not json");
  assert(invalidJsonStatus === 400, `invalid JSON body should 400, got ${invalidJsonStatus}`);

  const arrayBodyStatus = await postRawStatus("/api/w/main/projects", "[]");
  assert(arrayBodyStatus === 400, `a non-object JSON body should 400, got ${arrayBodyStatus}`);

  const badTxidStatus = await postStatus("/api/w/main/projects", { name: "x", txid: "nope" });
  assert(badTxidStatus === 400, `a malformed txid should 400, got ${badTxidStatus}`);

  const badFieldStatus = await postStatus("/api/w/main/projects", { name: 42 });
  assert(badFieldStatus === 400, `a non-string project name should 400, got ${badFieldStatus}`);

  const badStatusStatus = await postStatus("/api/w/main/issues", {
    projectId,
    title: "bad status",
    status: "archived",
  });
  assert(badStatusStatus === 400, `a status outside the schema should 400, got ${badStatusStatus}`);

  const unknownStatus = await postStatus("/api/w/nope12345/projects", { name: "x" });
  assert(unknownStatus === 404, `unknown workspace should 404, got ${unknownStatus}`);

  const malformedStatus = await postStatus("/api/w/-bad/projects", { name: "x" });
  assert(malformedStatus === 400, `malformed workspace id should 400, got ${malformedStatus}`);

  // === 6. Contention: parallel writes converge via CAS retry ===

  const before = await readStream(mainStreamUrl);
  const contentionCount = 10;
  const contentionResults = await Promise.all(
    Array.from({ length: contentionCount }, (_, index) =>
      postJson("/api/w/main/issues", {
        projectId,
        title: `Contention issue ${index + 1}`,
      }),
    ),
  );
  const contentionIds = contentionResults.map((result) => {
    assert(result.issue?.id, "contended issue create should return an issue id");
    return result.issue.id;
  });
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

  // === 7. Direct CAS conflict probe over HTTP ===

  const { head } = await readStream(mainStreamUrl);
  const casMatch = await fetch(mainStreamUrl, {
    method: "POST",
    headers: { "content-type": "application/json", "stream-expected-offset": head },
    body: JSON.stringify(probeEvent("a")),
  });
  assert(
    casMatch.status >= 200 && casMatch.status < 300,
    `append with matching expectedOffset should succeed, got ${casMatch.status}`,
  );

  const casStale = await fetch(mainStreamUrl, {
    method: "POST",
    headers: { "content-type": "application/json", "stream-expected-offset": head },
    body: JSON.stringify(probeEvent("b")),
  });
  assert(casStale.status === 409, `stale expectedOffset should 409, got ${casStale.status}`);
  assert(
    casStale.headers.get("stream-next-offset"),
    "CAS conflict should report the actual tail in stream-next-offset",
  );
  assert(
    casStale.headers.get("stream-closed") !== "true",
    "CAS conflict must be distinguishable from the closed-stream 409",
  );

  // === 8. SQLite restart preserves a workspace and its issue ===

  const restartWorkspaceId = await createWorkspace();
  const restartStreamUrl = `${baseUrl}/streams/workspace/${restartWorkspaceId}`;
  const beforeRestartIssue = await readStream(restartStreamUrl);
  const restartProject = projectSchema.parse(beforeRestartIssue.events[0]?.value);
  const restartIssue = await postJson(`/api/w/${restartWorkspaceId}/issues`, {
    projectId: restartProject.id,
    title: "Survives restart",
  });
  const restartIssueId = restartIssue.issue?.id;
  assert(restartIssueId, "restart fixture should create an issue");

  server.kill();
  await server.exited;
  server = startServer();
  await waitForServer();

  const afterRestart = await readStream(restartStreamUrl);
  findEvent(afterRestart.events, "issue", restartIssueId, "upsert");
  assert(
    afterRestart.head === restartIssue.awaitOffset,
    "workspace head after restart should match the persisted issue append",
  );

  console.log(
    `issue-tracker-demo HTTP smoke passed: main project ${projectId}, issue ${issueId}, ` +
      `shared workspace ${workspaceId}, ${after.events.length + 1} main stream events`,
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
