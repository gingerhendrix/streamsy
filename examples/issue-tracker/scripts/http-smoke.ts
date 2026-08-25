/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/node-builtin-import -- This is an executable smoke script: it starts a real server, drives it over real HTTP with the Promise-native client, and reports to the invoking terminal. */
/* oxlint-disable anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-unknown-parameters -- The script reads its own server's declared wire contracts back over HTTP and asserts on them; the assertion on each parsed body IS the check, and a failed one exits non-zero. */
/**
 * HTTP smoke.
 *
 * Starts a real Bun server backed by on-disk SQLite, drives the whole slice
 * over the network, restarts the host, and checks offset-based sink resume
 * with the ordinary Durable Streams client. It asserts; a failure exits
 * non-zero.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createLocalHost, type LocalHostOptions } from "../server/local.ts";
import { createBoardConnection } from "../src/lib/board-db.ts";

const checks: string[] = [];

function check(label: string, condition: boolean, detail?: unknown): void {
  if (!condition) {
    console.error(`FAIL ${label}`, detail === undefined ? "" : detail);
    process.exit(1);
  }
  checks.push(label);
  console.log(`ok   ${label}`);
}

interface Running {
  readonly host: ReturnType<typeof createLocalHost>;
  readonly server: ReturnType<typeof Bun.serve>;
  readonly origin: string;
}

function start(options: LocalHostOptions): Running {
  const host = createLocalHost(options);
  const server = Bun.serve({ port: 0, fetch: host.fetch, idleTimeout: 30 });
  return { host, server, origin: `http://localhost:${server.port}` };
}

async function stop(running: Running): Promise<void> {
  await running.server.stop(true);
  await running.host.close();
}

async function post(origin: string, path: string, body: unknown): Promise<Response> {
  return fetch(`${origin}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

const directory = mkdtempSync(join(tmpdir(), "issue-tracker-smoke-"));
let running = start({ databaseDirectory: directory });

try {
  const health = (await (await fetch(`${running.origin}/health`)).json()) as {
    status: string;
    view: string;
    planHash: string;
  };
  check("health reports the declaration", health.status === "ok", health);
  check("health carries the plan identity", /^[0-9a-f]{8}$/.test(health.planHash), health);

  const seeded = (await (await post(running.origin, "/api/workspaces/main/seed", {})).json()) as {
    seeded: boolean;
    issues: string[];
  };
  check("seeding fills the board", seeded.seeded && seeded.issues.length === 4, seeded);

  const createBody = {
    commandId: "smoke-create",
    issueId: "smoke-issue",
    projectId: "streamsy",
    title: "Smoke the whole path",
    status: "todo",
  };
  const created = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const createdBody = (await created.json()) as {
    ack: { offset: string };
    reconciled: boolean;
    row: { status: string } | null;
  };
  check("a command is accepted", created.status === 201 && !createdBody.reconciled, createdBody);
  check("the maintained row is in the declared column", createdBody.row?.status === "todo");

  const retried = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const retriedBody = (await retried.json()) as {
    ack: { offset: string };
    reconciled: boolean;
    maintenance: { folded: number };
  };
  check(
    "a retried commandId reports the original acceptance",
    retried.status === 200 &&
      retriedBody.reconciled &&
      retriedBody.ack.offset === createdBody.ack.offset &&
      retriedBody.maintenance.folded === 0,
    retriedBody,
  );

  const project = await post(running.origin, "/api/workspaces/main/catalog/projects", {
    key: "streamsy",
    value: {
      projectId: "streamsy",
      workspaceId: "main",
      key: "STR",
      name: "Streamsy",
      updatedAt: "2026-08-24T10:00:00.000Z",
    },
  });
  const projectBody = (await project.json()) as { rows: unknown[]; changed: number };
  check(
    "a State upsert maintains one current project row",
    project.status === 200 && projectBody.rows.length === 1 && projectBody.changed === 1,
    projectBody,
  );

  // === the sink product, read the way a browser reads it ===
  const connection = createBoardConnection({
    workspaceId: "main",
    origin: running.origin,
    onStatus: () => undefined,
  });
  await connection.preload();
  const synced = connection.db.collections.issues.toArray as { issueId: string; status: string }[];
  check(
    "the TanStack DB collection holds the maintained rows",
    synced.length === 5 && synced.some((row) => row.issueId === "smoke-issue"),
    synced.map((row) => row.issueId),
  );

  await post(running.origin, "/api/workspaces/main/issues/smoke-issue/status", {
    commandId: "smoke-move",
    status: "done",
  });
  const deadline = Date.now() + 10_000;
  let live = false;
  while (Date.now() < deadline) {
    const rows = connection.db.collections.issues.toArray as { issueId: string; status: string }[];
    if (rows.find((row) => row.issueId === "smoke-issue")?.status === "done") {
      live = true;
      break;
    }
    await Bun.sleep(25);
  }
  check("a move reaches the live consumer without a refresh", live);
  connection.close();

  const concurrent = await Promise.all([
    post(running.origin, "/api/workspaces/main/issues", {
      commandId: "smoke-concurrent-a",
      issueId: "smoke-concurrent-a",
      projectId: "streamsy",
      title: "Concurrent A",
    }),
    post(running.origin, "/api/workspaces/main/issues", {
      commandId: "smoke-concurrent-b",
      issueId: "smoke-concurrent-b",
      projectId: "streamsy",
      title: "Concurrent B",
    }),
  ]);
  const concurrentBodies = (await Promise.all(concurrent.map((response) => response.json()))) as {
    sequence: number;
  }[];
  check(
    "concurrent commands receive distinct source sequences",
    concurrent.every((response) => response.status === 201) &&
      new Set(concurrentBodies.map((body) => body.sequence)).size === 2,
    concurrentBodies,
  );

  // === offset-based sink resume ===
  const session = (await (
    await fetch(`${running.origin}/api/workspaces/main/sink-session`)
  ).json()) as { offset: string; fallback: string };
  const suffixBefore = await fetch(
    `${running.origin}/state/workspaces/main/issues?offset=${encodeURIComponent(session.offset)}`,
  );
  const emptySuffix = (await suffixBefore.json()) as unknown[];
  check("resuming at the tail replays nothing", emptySuffix.length === 0, emptySuffix);

  await post(running.origin, "/api/workspaces/main/issues", {
    commandId: "smoke-after-resume",
    issueId: "smoke-later",
    projectId: "streamsy",
    title: "Appended after the offset",
    status: "backlog",
  });
  const suffix = (await (
    await fetch(
      `${running.origin}/state/workspaces/main/issues?offset=${encodeURIComponent(session.offset)}`,
    )
  ).json()) as { type?: string; key?: string }[];
  check(
    "a native offset replays exactly the suffix",
    suffix
      .filter((message) => message.type === "issue")
      .every((message) => message.key === "smoke-later"),
    suffix,
  );

  // === restart ===
  await stop(running);
  running = start({ databaseDirectory: directory });

  const afterRestart = (await (
    await fetch(`${running.origin}/api/workspaces/main/issues`)
  ).json()) as { rows: { issueId: string; status: string }[] };
  check(
    "a restart preserves the maintained rows",
    afterRestart.rows.length === 8 &&
      afterRestart.rows.find((row) => row.issueId === "smoke-issue")?.status === "done",
    afterRestart.rows.map((row) => row.issueId),
  );

  const afterRestartRetry = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const afterRestartBody = (await afterRestartRetry.json()) as {
    reconciled: boolean;
    ack: { offset: string };
    maintenance: { folded: number };
  };
  check(
    "the receipt survives the restart, so a retry still reconciles",
    afterRestartBody.reconciled &&
      afterRestartBody.ack.offset === createdBody.ack.offset &&
      afterRestartBody.maintenance.folded === 0,
    afterRestartBody,
  );

  console.log(`\n${checks.length} checks passed`);
} finally {
  await stop(running);
}
