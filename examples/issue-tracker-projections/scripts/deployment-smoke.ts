/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/global-fetch, effecttsgo/global-random, effecttsgo/global-timers, effecttsgo/new-promise, effecttsgo/process-env -- This standalone Bun smoke executable reads its target URL from the process environment, derives a disposable workspace suffix, drives the deployed HTTP surface with Web fetch, waits between convergence polls on a self-contained timer, and reports its evidence to the invoking terminal. */
/**
 * Deployment smoke against a live URL.
 *
 * Run with `DEMO_URL=https://... bun run smoke:deployment`. It uses a
 * disposable workspace, never resets shared state, and reports the workspace id
 * so the evidence can be traced.
 */
import type {
  BoardResponse,
  CoverageResponse,
  MutationResponse,
  RepairResponse,
} from "../shared/api.ts";

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
const html = await shell.text();
assert(html.includes('id="root"'), "the SPA shell must carry the app mount point");
// Every asset the shell references must be reachable, hashed names included.
const referenced = [...html.matchAll(/(?:src|href)="([^"]+)"/g)].map((match) => match[1]!);
assert(referenced.length > 0, "the shell must reference bundled assets");
for (const asset of referenced) {
  const response = await call("GET", asset.startsWith("/") ? asset : `/${asset}`);
  assert(response.status === 200, `asset ${asset} must load, got ${response.status}`);
}

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

// The command that matters: `?projections=deferred` skips both immediate
// passes, so this exercises exactly the case a lost immediate pass produces.
// Durability and the acknowledgement are unchanged.
const updated: MutationResponse = await json(
  await call(
    "POST",
    `/api/workspaces/${workspaceId}/issues/${issueId}/commands?projections=deferred`,
    { commandId: `${suffix}-done`, type: "status", status: "done" },
  ),
);
assert(updated.coverage.hops.length === 2, "coverage must report both hops");
assert(updated.ack.position.length > 0, "a deferred command must still be acknowledged exactly");
assert(
  updated.projections.every((pass) => pass.outcome === "deferred"),
  `both passes must be deferred, got ${JSON.stringify(updated.projections)}`,
);

// Convergence must not depend on the mutation request: it has to come from the
// queue consumer or the explicit repair endpoint doing the same bounded work.
let converged: BoardResponse | undefined;
let proven: CoverageResponse | undefined;
const coveragePath = `/api/workspaces/${workspaceId}/issues/${issueId}/coverage?position=${encodeURIComponent(
  updated.ack.position,
)}`;
for (let attempt = 0; attempt < 10; attempt++) {
  const repaired: RepairResponse = await json(
    await call("POST", `/api/workspaces/${workspaceId}/projects/${projectId}/repair`),
  );
  const faulted = repaired.projections.filter((pass) => pass.outcome === "faulted");
  assert(faulted.length === 0, `repair reported faulted passes: ${JSON.stringify(faulted)}`);

  const probe: CoverageResponse = await json(await call("GET", coveragePath));
  const board: BoardResponse = await json(
    await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
  );
  if (
    probe.coverage.status === "proven" &&
    board.rows.some((row) => row.issueId === issueId && row.status === "done")
  ) {
    proven = probe;
    converged = board;
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 1_000));
}
assert(converged !== undefined, "the board must converge on the durable status");
assert(proven !== undefined, "chained coverage must reach proven after repair");
// The whole point of the run: durable lineage, not elapsed time, says proven.
assert(
  proven.coverage.status === "proven",
  `coverage.status must be proven, got ${proven.coverage.status}`,
);

const streamed = await call("GET", `/streams/${converged.boardStream}`);
assert(streamed.status === 200, `public board stream read must succeed, got ${streamed.status}`);

// Evidence limit, stated precisely: this is a *second HTTP request* observing
// the same durable rows. It does not force a new Worker isolate or a Durable
// Object eviction, so it is not evidence of cold re-entry. Deploying a new
// version, or waiting out an isolate, is the only way to test that here.
const reread: BoardResponse = await json(
  await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
);
assert(
  reread.rows.length === converged.rows.length,
  "a second request must observe the same durable rows",
);

console.log(
  JSON.stringify(
    {
      url: base,
      workspaceId,
      projectId,
      issueId,
      ack: created.ack,
      deferredAck: updated.ack,
      coverage: proven.coverage.status,
      reEntryEvidence: "second-request-same-durable-rows (not a cold isolate)",
    },
    null,
    2,
  ),
);

function call(method: string, path: string, body?: unknown): Promise<Response> {
  return fetch(`${base}${path}`, {
    method,
    ...(body === undefined
      ? {}
      : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
  });
}

/**
 * Read a JSON body.
 *
 * The type parameter names the response contract the caller expects; the
 * server builds that contract from the shared Schemas, and the assertions in
 * this smoke are what check it.
 */
async function json<T = any>(response: Response): Promise<T> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 400)}`);
  return JSON.parse(text);
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
