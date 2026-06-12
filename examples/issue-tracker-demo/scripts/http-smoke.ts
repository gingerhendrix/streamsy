import { resolve } from "node:path";

const packageDir = resolve(import.meta.dir, "..");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;
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

interface ChangeEvent {
  type: string;
  key: string;
  value: Record<string, unknown>;
  old_value?: Record<string, unknown>;
  headers: { operation: string; txid: string; timestamp: string };
}

interface MutationResult {
  awaitOffset: string;
  txid: string;
  project?: { id: string };
  issue?: { id: string; status: string };
  comment?: { id: string };
}

async function postJson(
  path: string,
  body: unknown,
  method: "POST" | "PATCH" = "POST",
): Promise<MutationResult> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  assert(response.ok, `${method} ${path} expected 2xx, got ${response.status}: ${text}`);
  return JSON.parse(text) as MutationResult;
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

  throw new SmokeError(`Issue tracker server did not become ready: ${lastError}`);
}

async function readStream(): Promise<ChangeEvent[]> {
  const response = await fetch(`${streamUrl}?offset=-1`);
  assert(response.status === 200, `stream read status ${response.status}`);
  assert(response.headers.get("stream-up-to-date") === "true", "stream read should be up to date");
  const body = (await response.json()) as ChangeEvent[];
  assert(Array.isArray(body), "stream read body should be a JSON array");
  return body;
}

function findEvent(
  events: ChangeEvent[],
  type: string,
  key: string,
  operation: string,
): ChangeEvent {
  const match = events.find(
    (event) => event.type === type && event.key === key && event.headers.operation === operation,
  );
  assert(match, `expected a ${operation} ${type} event for ${key} in the stream`);
  return match;
}

const server = Bun.spawn(["bun", "src/server/index.ts"], {
  cwd: packageDir,
  env: { ...process.env, PORT: String(port) },
  stdout: "pipe",
  stderr: "pipe",
});

try {
  await waitForServer();

  // 1. Create a project.
  const projectResult = await postJson("/api/projects", {
    name: "Smoke Project",
    description: "created by http-smoke",
  });
  const projectId = projectResult.project?.id;
  assert(projectId, "create project should return a project id");
  assert(projectResult.txid, "create project should return a txid");
  assert(projectResult.awaitOffset, "create project should return an awaitOffset");

  // 2. Reject an issue with an unknown projectId (validation path).
  const badIssue = await fetch(`${baseUrl}/api/issues`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ projectId: "proj_does_not_exist", title: "nope" }),
  });
  assert(badIssue.status === 400, `unknown projectId should 400, got ${badIssue.status}`);

  // 3. Create an issue under the project.
  const issueResult = await postJson("/api/issues", {
    projectId,
    title: "Smoke issue",
  });
  const issueId = issueResult.issue?.id;
  assert(issueId, "create issue should return an issue id");
  assert(issueResult.issue?.status === "open", "new issue should default to open");

  // 4. Update the issue status (create/update flow).
  const updateResult = await postJson(
    `/api/issues/${encodeURIComponent(issueId)}`,
    { status: "done" },
    "PATCH",
  );
  assert(updateResult.issue?.status === "done", "issue update should set status to done");

  // 5. Comment on the issue.
  const commentResult = await postJson("/api/comments", {
    issueId,
    body: "looks good",
  });
  const commentId = commentResult.comment?.id;
  assert(commentId, "create comment should return a comment id");

  // 6. Read the durable stream and verify every mutation produced a client-readable
  //    Durable State change event.
  const events = await readStream();

  const projectEvent = findEvent(events, "project", projectId, "upsert");
  assert(projectEvent.value.id === projectId, "project event value should carry the project id");

  findEvent(events, "issue", issueId, "upsert");

  const issueUpdate = findEvent(events, "issue", issueId, "update");
  assert(issueUpdate.value.status === "done", "issue update event should carry status=done");
  assert(issueUpdate.old_value, "issue update event should include old_value for replication");

  const commentEvent = findEvent(events, "comment", commentId, "upsert");
  assert(commentEvent.value.issueId === issueId, "comment event value should reference its issue");

  // Every event must be a well-formed Durable State change event a client can replay.
  for (const event of events) {
    assert(typeof event.type === "string" && event.type.length > 0, "event missing type");
    assert(typeof event.key === "string" && event.key.length > 0, "event missing key");
    assert(event.value && typeof event.value === "object", "event missing value");
    assert(typeof event.headers?.operation === "string", "event missing headers.operation");
  }

  console.log(
    `issue-tracker-demo HTTP smoke passed: project ${projectId}, issue ${issueId}, ${events.length} stream events`,
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
}
