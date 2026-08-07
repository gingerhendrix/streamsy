/**
 * Deployment smoke against a live URL.
 *
 * Run with `DEMO_URL=https://... bun run smoke:deployment`. It uses a
 * disposable workspace, never resets shared state, and reports the workspace id
 * so the evidence can be traced.
 */
import type { BoardResponse, MutationResponse } from "../shared/api.ts";

const base = (process.env.DEMO_URL ?? "").replace(/\/$/, "");
if (base.length === 0) throw new Error("DEMO_URL is required");

const suffix = Math.random().toString(36).slice(2, 8);
const workspaceId = `smoke${suffix}`;
const projectId = `proj${suffix}`;
const issueId = `issue${suffix}`;

const health = await json(await call("GET", "/health"));
assert(health.status === "ok", "health must be ok");
console.log(`deployment ${health.deployment} schema ${health.schemaVersion}`);

const shell = await call("GET", "/");
assert(shell.status === 200, `SPA shell must load, got ${shell.status}`);
assert((await call("GET", "/styles.css")).status === 200, "stylesheet must load");

await call("POST", `/api/workspaces/${workspaceId}/projects`, {
  projectId,
  projectKey: "SMK",
  name: "Deployment smoke",
});

const created: MutationResponse = await json(
  await call("POST", `/api/workspaces/${workspaceId}/issues`, {
    commandId: `${suffix}-create`,
    issueId,
    projectId,
    title: "Deployment smoke",
    priority: "high",
  }),
);
assert(created.ack.position.length > 0, "create must return an exact acknowledgement");

const updated: MutationResponse = await json(
  await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/commands`, {
    commandId: `${suffix}-done`,
    type: "status",
    status: "done",
  }),
);
assert(updated.coverage.hops.length === 2, "coverage must report both hops");

// Convergence must not depend on the mutation request: wait for the durable
// board through repair/queue catch-up instead.
let converged: BoardResponse | undefined;
for (let attempt = 0; attempt < 10; attempt++) {
  await call("POST", `/api/workspaces/${workspaceId}/projects/${projectId}/repair`);
  const board: BoardResponse = await json(
    await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
  );
  if (board.rows.some((row) => row.issueId === issueId && row.status === "done")) {
    converged = board;
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 1_000));
}
assert(converged !== undefined, "the board must converge on the durable status");

const streamed = await call("GET", `/streams/${converged.boardStream}`);
assert(streamed.status === 200, `public board stream read must succeed, got ${streamed.status}`);

// A fresh request after the durable write must still see the same state.
const reread: BoardResponse = await json(
  await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
);
assert(reread.rows.length === converged.rows.length, "durable state must survive re-entry");

console.log(
  JSON.stringify({ url: base, workspaceId, projectId, issueId, ack: created.ack }, null, 2),
);

function call(method: string, path: string, body?: unknown): Promise<Response> {
  return fetch(`${base}${path}`, {
    method,
    ...(body === undefined
      ? {}
      : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
  });
}

async function json<T = any>(response: Response): Promise<T> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 400)}`);
  return JSON.parse(text) as T;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
