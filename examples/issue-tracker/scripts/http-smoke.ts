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
import { createLabelCountsConnection } from "../src/lib/label-counts-db.ts";

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

  // === the effect sink: one assignment, one delivery, across a restart ===
  const assigned = (await (
    await post(running.origin, "/api/workspaces/main/issues/smoke-issue/assignee", {
      commandId: "smoke-assign",
      assigneeId: "ada",
    })
  ).json()) as { row: { assigneeId?: string } | null };
  check("an assignment maintains the row", assigned.row?.assigneeId === "ada", assigned);

  const queued = (await (
    await fetch(`${running.origin}/api/workspaces/main/notifications`)
  ).json()) as { pending: number; delivered: number; outbox: { idempotencyKey: string }[] };
  check(
    "the accepted command enqueued exactly one durable delivery",
    queued.pending === 1 && queued.delivered === 0 && queued.outbox.length === 1,
    queued,
  );

  const retriedAssign = await post(
    running.origin,
    "/api/workspaces/main/issues/smoke-issue/assignee",
    { commandId: "smoke-assign", assigneeId: "ada" },
  );
  const afterRetriedAssign = (await (
    await fetch(`${running.origin}/api/workspaces/main/notifications`)
  ).json()) as { outbox: unknown[] };
  check(
    "a retried assignment adds no second delivery",
    retriedAssign.status === 200 && afterRetriedAssign.outbox.length === 1,
    afterRetriedAssign,
  );

  const drained = (await (
    await post(running.origin, "/api/workspaces/main/notifications/drain", {})
  ).json()) as { claimed: number; delivered: number; deadLettered: number };
  check(
    "draining the lane performs the delivery once",
    drained.claimed === 1 && drained.delivered === 1 && drained.deadLettered === 0,
    drained,
  );
  const redrained = (await (
    await post(running.origin, "/api/workspaces/main/notifications/drain", {})
  ).json()) as { claimed: number };
  check("a delivered effect is never repeated", redrained.claimed === 0, redrained);

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

  // === the stream sink's activity feed ===
  const feedPath = "/feed/workspaces/main/issue-transitions";
  const feedResponse = await fetch(`${running.origin}${feedPath}`);
  const feedPage = (await feedResponse.json()) as {
    order: string;
    events: { issueId: string; change: string }[];
    nextOffset: string;
    upToDate: boolean;
  };
  check(
    "the transition feed serves arrival order over HTTP",
    feedResponse.status === 200 &&
      feedPage.order === "arrival" &&
      feedPage.upToDate &&
      feedPage.events.some((event) => event.issueId === "smoke-later"),
    feedPage.events.map((event) => `${event.change}:${event.issueId}`),
  );

  // The feed's own cursor must replay exactly what was appended after it.
  const feedTail = (await (
    await fetch(`${running.origin}${feedPath}?offset=${encodeURIComponent(feedPage.nextOffset)}`)
  ).json()) as { events: unknown[] };
  check("resuming the feed at its tail replays nothing", feedTail.events.length === 0, feedTail);

  const refusedFeed = await fetch(`${running.origin}${feedPath}?offset=not-an-offset`);
  check(
    "an unusable feed offset declares replay-from-start",
    refusedFeed.status === 409 &&
      ((await refusedFeed.json()) as { recovery?: string }).recovery === "replay-from-start",
    refusedFeed.status,
  );

  // === the document sink's cached workspace summary ===
  const summaryPath = "/document/workspaces/main/summary";
  const summaryResponse = await fetch(`${running.origin}${summaryPath}`);
  const summaryDocument = (await summaryResponse.clone().json()) as {
    workspaceId: string;
    issues: { total: number };
  };
  const summaryEtag = summaryResponse.headers.get("etag") ?? "";
  check(
    "the workspace summary serves its declared cache policy",
    summaryResponse.status === 200 &&
      summaryResponse.headers.get("cache-control") === "private, max-age=0, must-revalidate" &&
      /^"[0-9a-f]{16}"$/.test(summaryEtag) &&
      summaryDocument.workspaceId === "main" &&
      summaryDocument.issues.total === 8,
    { etag: summaryEtag, total: summaryDocument.issues.total },
  );

  const conditional = await fetch(`${running.origin}${summaryPath}`, {
    headers: { "if-none-match": summaryEtag },
  });
  check(
    "a conditional summary request is a bodiless 304",
    conditional.status === 304 && (await conditional.text()) === "",
    conditional.status,
  );

  // === a second workspace in the same host ===
  const opsSeed = (await (await post(running.origin, "/api/workspaces/ops/seed", {})).json()) as {
    seeded: boolean;
    issues: string[];
  };
  check(
    "a second workspace seeds in the same host",
    opsSeed.seeded && opsSeed.issues.length === 4,
    opsSeed,
  );

  const opsIssues = (await (await fetch(`${running.origin}/api/workspaces/ops/issues`)).json()) as {
    rows: { issueId: string }[];
  };
  check(
    "the second workspace holds only its own rows",
    opsIssues.rows.length === 4 && !opsIssues.rows.some((row) => row.issueId === "smoke-issue"),
    opsIssues.rows.map((row) => row.issueId),
  );

  const opsSummary = (await (
    await fetch(`${running.origin}/document/workspaces/ops/summary`)
  ).json()) as { workspaceId: string; issues: { total: number } };
  check(
    "each workspace's document sink is served from its own partition",
    opsSummary.workspaceId === "ops" && opsSummary.issues.total === 4,
    opsSummary,
  );

  const metrics = (await (await fetch(`${running.origin}/host/metrics`)).json()) as {
    open: number;
    workspaces: { workspaceId: string; requests: number }[];
  };
  check(
    "host metrics report one partition per live workspace",
    metrics.open === 2 &&
      metrics.workspaces
        .map((entry) => entry.workspaceId)
        .toSorted()
        .join(",") === "main,ops",
    metrics,
  );

  // === label membership and the second checked State sink ===
  const seededLabels = (await (
    await fetch(`${running.origin}/api/workspaces/main/label-counts`)
  ).json()) as { rows: { labelId: string; issueCount: number }[]; contractFingerprint: string };
  const countOf = (rows: { labelId: string; issueCount: number }[], labelId: string): number =>
    rows.find((row) => row.labelId === labelId)?.issueCount ?? 0;
  check(
    "a seeded workspace opens onto live label counts",
    countOf(seededLabels.rows, "infra") === 2 && countOf(seededLabels.rows, "docs") === 1,
    seededLabels,
  );

  /**
   * The label-count binding is the second generated client contract, and it is
   * bound here over the real network exactly as the browser binds it — so what
   * this check proves is the published product, not the read model beside it.
   */
  const countsConnection = createLabelCountsConnection({
    workspaceId: "main",
    origin: running.origin,
    onStatus: () => undefined,
  });
  await countsConnection.preload();
  const boundCounts = [...countsConnection.db.collections.labelCounts.entries()].map(
    ([, row]) => row,
  );
  check(
    "the generated label-count binding holds the maintained counts",
    countOf(boundCounts, "infra") === 2,
    boundCounts,
  );

  const attached = (await (
    await post(running.origin, "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "smoke-attach",
      labelId: "bug",
    })
  ).json()) as { membershipId: string; attached: boolean; reconciled: boolean };
  check(
    "attaching a label appends a membership fact",
    attached.membershipId === "seed-plan.bug" && attached.attached && !attached.reconciled,
    attached,
  );
  await new Promise((resolve) => setTimeout(resolve, 200));
  const liveCounts = [...countsConnection.db.collections.labelCounts.entries()].map(([, r]) => r);
  check(
    "the attach reaches the live consumer without a refresh",
    countOf(liveCounts, "bug") === 2,
    liveCounts,
  );

  const retriedAttach = (await (
    await post(running.origin, "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "smoke-attach",
      labelId: "bug",
    })
  ).json()) as { reconciled: boolean };
  check("a retried membership command appends nothing", retriedAttach.reconciled, retriedAttach);

  const detached = (await (
    await post(running.origin, "/api/workspaces/main/issues/seed-plan/labels/detach", {
      commandId: "smoke-detach",
      labelId: "bug",
    })
  ).json()) as { attached: boolean };
  const afterDetach = (await (
    await fetch(`${running.origin}/api/workspaces/main/label-counts`)
  ).json()) as { rows: { labelId: string; issueCount: number }[] };
  check(
    "detaching removes the count with no State delete",
    !detached.attached && countOf(afterDetach.rows, "bug") === 1,
    afterDetach,
  );
  const memberships = (await (
    await fetch(`${running.origin}/api/workspaces/main/issue-labels`)
  ).json()) as { rows: { membershipId: string; attached: boolean }[] };
  check(
    "the detached membership stays in the relation, marked detached",
    memberships.rows.find((row) => row.membershipId === "seed-plan.bug")?.attached === false,
    memberships.rows,
  );
  countsConnection.close();

  const bothSinks = (await (
    await fetch(`${running.origin}/api/workspaces/main/sink-session`)
  ).json()) as { labelCounts: { sink: string; contractFingerprint: string } };
  check(
    "the sink session names both checked State contracts",
    bothSinks.labelCounts.sink === "issue-tracker.board-label-counts" &&
      bothSinks.labelCounts.contractFingerprint === seededLabels.contractFingerprint,
    bothSinks,
  );

  // === the cross-domain exchange: two workspaces into one user inbox ===
  const opsAssigned = (await (
    await post(running.origin, "/api/workspaces/ops/issues/seed-plan/assignee", {
      commandId: "smoke-assign-ops",
      assigneeId: "ada",
    })
  ).json()) as { row: { assigneeId?: string } | null };
  check("the second workspace accepts its own assignment", opsAssigned.row?.assigneeId === "ada");

  const exchanged = await running.host.host.exchange();
  check(
    "one exchange pass reads every open workspace",
    exchanged.length === 2 && exchanged.every((report) => !report.failed),
    exchanged,
  );

  const inbox = (await (await fetch(`${running.origin}/api/users/ada/inbox`)).json()) as {
    userId: string;
    rows: { workspaceId: string; issueId: string; userId: string }[];
  };
  check(
    "one user inbox is fed by two workspace domains",
    inbox.userId === "ada" &&
      inbox.rows.length === 2 &&
      inbox.rows.every((row) => row.userId === "ada") &&
      inbox.rows
        .map((row) => row.workspaceId)
        .toSorted()
        .join(",") === "main,ops",
    inbox,
  );

  const cursors = (await (await fetch(`${running.origin}/api/global/exchange`)).json()) as {
    cursors: { domain: string; source: { kind: string; id: string }; applied: number }[];
  };
  check(
    "the exchange resumes on its own cursor domain",
    cursors.cursors.length === 2 &&
      cursors.cursors.every(
        (cursor) =>
          cursor.domain === "issue-tracker.exchange-cursor/1" && cursor.source.kind === "workspace",
      ) &&
      cursors.cursors.reduce((sum, cursor) => sum + cursor.applied, 0) === 2,
    cursors,
  );

  // === restart ===
  await stop(running);
  running = start({ databaseDirectory: directory });

  const afterRestart = (await (
    await fetch(`${running.origin}/api/workspaces/main/issues`)
  ).json()) as { rows: { issueId: string; status: string; assigneeId?: string }[] };
  check(
    "a restart preserves the maintained rows",
    afterRestart.rows.length === 8 &&
      afterRestart.rows.find((row) => row.issueId === "smoke-issue")?.status === "done" &&
      afterRestart.rows.find((row) => row.issueId === "smoke-issue")?.assigneeId === "ada",
    afterRestart.rows.map((row) => row.issueId),
  );

  const afterRestartRetry = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const afterRestartBody = (await afterRestartRetry.json()) as {
    reconciled: boolean;
    ack: { offset: string };
    maintenance: { folded: number };
  };
  const notificationsAfterRestart = (await (
    await fetch(`${running.origin}/api/workspaces/main/notifications`)
  ).json()) as { pending: number; delivered: number; notified: { assigneeId: string }[] };
  check(
    "the outbox survives the restart with its settled state intact",
    notificationsAfterRestart.pending === 0 && notificationsAfterRestart.delivered === 1,
    notificationsAfterRestart,
  );
  const redrainedAfterRestart = (await (
    await post(running.origin, "/api/workspaces/main/notifications/drain", {})
  ).json()) as { claimed: number };
  check(
    "a restarted host re-delivers nothing that was already delivered",
    redrainedAfterRestart.claimed === 0,
    redrainedAfterRestart,
  );

  const inboxAfterRestart = (await (
    await fetch(`${running.origin}/api/users/ada/inbox`)
  ).json()) as { rows: { workspaceId: string }[] };
  check(
    "the user inbox survives the restart",
    inboxAfterRestart.rows.length === 2,
    inboxAfterRestart,
  );

  await fetch(`${running.origin}/api/workspaces/ops/issues`);
  const exchangedAfterRestart = await running.host.host.exchange();
  check(
    "a restarted host re-exchanges nothing that was already applied",
    exchangedAfterRestart.length === 2 &&
      exchangedAfterRestart.every((report) => !report.failed && report.applied === 0),
    exchangedAfterRestart,
  );

  const labelsAfterRestart = (await (
    await fetch(`${running.origin}/api/workspaces/main/label-counts`)
  ).json()) as { rows: { labelId: string; issueCount: number }[] };
  check(
    "label counts survive the restart",
    labelsAfterRestart.rows.find((row) => row.labelId === "infra")?.issueCount === 2 &&
      labelsAfterRestart.rows.find((row) => row.labelId === "bug")?.issueCount === 1,
    labelsAfterRestart,
  );

  /**
   * The cold-source policy, over the network: give every partition up, then run
   * one exchange pass and assert it still found its source. Before Integration 2
   * this pass had nothing to read at all.
   */
  await post(running.origin, "/api/workspaces/main/issues/seed-scale/assignee", {
    commandId: "smoke-assign-cold",
    assigneeId: "grace",
  });
  await running.host.host.sweepIdle(Date.now() + 3_600_000);
  check(
    "every partition was given up before the cold pass",
    running.host.host.openPartitions().length === 0,
    running.host.host.openPartitions(),
  );
  const coldPass = await running.host.host.exchange();
  const graceInbox = (await (await fetch(`${running.origin}/api/users/grace/inbox`)).json()) as {
    rows: { issueId: string }[];
  };
  check(
    "a closed workspace is still exchanged into its assignee's inbox",
    coldPass.some((report) => report.source.id === "main" && report.applied === 1) &&
      graceInbox.rows.some((row) => row.issueId === "seed-scale"),
    { coldPass, graceInbox },
  );

  const registered = (await (await fetch(`${running.origin}/api/global/sources`)).json()) as {
    sources: { partition: string }[];
  };
  check(
    "the durable source registry survived the restart",
    registered.sources
      .map((source) => source.partition)
      .toSorted()
      .join(",") === "workspace:main,workspace:ops",
    registered,
  );

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
