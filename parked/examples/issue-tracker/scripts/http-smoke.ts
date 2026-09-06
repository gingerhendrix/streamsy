/**
 * HTTP smoke.
 *
 * Starts a real Bun server backed by on-disk SQLite, drives the whole slice
 * over the network, restarts the host, and checks offset-based sink resume
 * with the ordinary Durable Streams client. It asserts; a failure exits
 * non-zero.
 */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This executable creates an isolated native temporary directory for its restart-persistence checks.
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This executable builds the isolated database path through Bun's Node-compatible path API.
import { join } from "node:path";
import { DateTime, Schema } from "effect";
import { createLocalHost, type LocalHostOptions } from "../server/host/bun/local.ts";
import { WorkspaceSummary } from "../domain/issue.ts";
import {
  CatalogRowsResponse,
  CommandResponse,
  DrainResponse,
  ExchangeStatusResponse,
  HealthResponse,
  InboxResponse,
  IssueLabelsResponse,
  IssuesResponse,
  LabelCommandResponse,
  LabelCountsResponse,
  NotificationsResponse,
  SeedResponse,
  SinkSessionResponse,
  TransitionFeedResponse,
} from "../shared/api.ts";
import { createBoardConnection } from "../src/lib/board-db.ts";
import { createLabelCountsConnection } from "../src/lib/label-counts-db.ts";
import { decodeResponse, request, requestJson } from "./http.ts";

const StateMessages = Schema.Array(
  Schema.Struct({
    type: Schema.optionalKey(Schema.String),
    key: Schema.optionalKey(Schema.String),
  }),
);

const FeedResumeError = Schema.Struct({ recovery: Schema.String });

const HostMetricsResponse = Schema.Struct({
  open: Schema.Finite,
  workspaces: Schema.Array(Schema.Struct({ workspaceId: Schema.String, requests: Schema.Finite })),
});

const SourceRegistryResponse = Schema.Struct({
  sources: Schema.Array(Schema.Struct({ partition: Schema.String })),
});

const countOf = (
  rows: readonly { readonly labelId: string; readonly issueCount: number }[],
  labelId: string,
): number => rows.find((row) => row.labelId === labelId)?.issueCount ?? 0;

const checks: string[] = [];

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This executable assertion boundary accepts already-decoded values from several contracts solely to preserve their native stderr diagnostics on failure.
function check(label: string, condition: boolean, detail?: unknown): void {
  if (!condition) {
    // oxlint-disable-next-line effecttsgo/global-console -- A failed smoke check must retain its stderr diagnostic before the executable exits non-zero.
    console.error(`FAIL ${label}`, detail === undefined ? "" : detail);
    process.exit(1);
  }
  checks.push(label);
  // oxlint-disable-next-line effecttsgo/global-console -- Per-check stdout is the smoke command's documented terminal result.
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

// oxlint-disable-next-line effecttsgo/async-function -- This executable edge awaits native Bun server shutdown and the host's Promise-based close contract in order.
async function stop(running: Running): Promise<void> {
  await running.server.stop(true);
  await running.host.close();
}

// oxlint-disable-next-line effecttsgo/async-function -- This Promise-native HTTP adapter preserves the smoke script's existing Response-returning POST boundary.
async function post(origin: string, path: string, body: Schema.Json): Promise<Response> {
  return request(`${origin}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

const directory = mkdtempSync(join(tmpdir(), "issue-tracker-smoke-"));
let running = start({ databaseDirectory: directory });

try {
  const health = await requestJson(HealthResponse, `${running.origin}/health`);
  check("health reports the declaration", health.status === "ok", health);
  check("health carries the plan identity", /^[0-9a-f]{8}$/.test(health.planHash), health);

  const seededResponse = await post(running.origin, "/api/workspaces/main/seed", {});
  const seeded = await decodeResponse(SeedResponse, seededResponse);
  check("seeding fills the board", seeded.seeded && seeded.issues.length === 4, seeded);

  const createBody = {
    commandId: "smoke-create",
    issueId: "smoke-issue",
    projectId: "streamsy",
    title: "Smoke the whole path",
    status: "todo",
  };
  const created = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const createdBody = await decodeResponse(CommandResponse, created);
  check("a command is accepted", created.status === 201 && !createdBody.reconciled, createdBody);
  check("the maintained row is in the declared column", createdBody.row?.status === "todo");

  const retried = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const retriedBody = await decodeResponse(CommandResponse, retried);
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
  const projectBody = await decodeResponse(CatalogRowsResponse, project);
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
  const synced = [...connection.db.collections.issues.entries()].map(([, row]) => row);
  check(
    "the TanStack DB collection holds the maintained rows",
    synced.length === 5 && synced.some((row) => row.issueId === "smoke-issue"),
    synced.map((row) => row.issueId),
  );

  await post(running.origin, "/api/workspaces/main/issues/smoke-issue/status", {
    commandId: "smoke-move",
    status: "done",
  });
  const deadline = DateTime.toEpochMillis(DateTime.nowUnsafe()) + 10_000;
  let live = false;
  while (DateTime.toEpochMillis(DateTime.nowUnsafe()) < deadline) {
    const rows = [...connection.db.collections.issues.entries()].map(([, row]) => row);
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
  const concurrentBodies = await Promise.all(
    concurrent.map((response) => decodeResponse(CommandResponse, response)),
  );
  check(
    "concurrent commands receive distinct source sequences",
    concurrent.every((response) => response.status === 201) &&
      new Set(concurrentBodies.map((body) => body.sequence)).size === 2,
    concurrentBodies,
  );

  // === the action sink: one assignment, one delivery, across a restart ===
  const assignedResponse = await post(
    running.origin,
    "/api/workspaces/main/issues/smoke-issue/assignee",
    {
      commandId: "smoke-assign",
      assigneeId: "ada",
    },
  );
  const assigned = await decodeResponse(CommandResponse, assignedResponse);
  check("an assignment maintains the row", assigned.row?.assigneeId === "ada", assigned);

  const queued = await requestJson(
    NotificationsResponse,
    `${running.origin}/api/workspaces/main/notifications`,
  );
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
  const afterRetriedAssign = await requestJson(
    NotificationsResponse,
    `${running.origin}/api/workspaces/main/notifications`,
  );
  check(
    "a retried assignment adds no second delivery",
    retriedAssign.status === 200 && afterRetriedAssign.outbox.length === 1,
    afterRetriedAssign,
  );

  const drainedResponse = await post(
    running.origin,
    "/api/workspaces/main/notifications/drain",
    {},
  );
  const drained = await decodeResponse(DrainResponse, drainedResponse);
  check(
    "draining the lane performs the delivery once",
    drained.claimed === 1 && drained.delivered === 1 && drained.deadLettered === 0,
    drained,
  );
  const redrainedResponse = await post(
    running.origin,
    "/api/workspaces/main/notifications/drain",
    {},
  );
  const redrained = await decodeResponse(DrainResponse, redrainedResponse);
  check("a delivered effect is never repeated", redrained.claimed === 0, redrained);

  // === offset-based sink resume ===
  const session = await requestJson(
    SinkSessionResponse,
    `${running.origin}/api/workspaces/main/sink-session`,
  );
  const suffixBefore = await request(
    `${running.origin}/state/workspaces/main/issues?offset=${encodeURIComponent(session.offset)}`,
  );
  const emptySuffix = await decodeResponse(StateMessages, suffixBefore);
  check("resuming at the tail replays nothing", emptySuffix.length === 0, emptySuffix);

  await post(running.origin, "/api/workspaces/main/issues", {
    commandId: "smoke-after-resume",
    issueId: "smoke-later",
    projectId: "streamsy",
    title: "Appended after the offset",
    status: "backlog",
  });
  const suffix = await requestJson(
    StateMessages,
    `${running.origin}/state/workspaces/main/issues?offset=${encodeURIComponent(session.offset)}`,
  );
  check(
    "a native offset replays exactly the suffix",
    suffix
      .filter((message) => message.type === "issue")
      .every((message) => message.key === "smoke-later"),
    suffix,
  );

  // === the stream sink's activity feed ===
  const feedPath = "/feed/workspaces/main/issue-transitions";
  const feedResponse = await request(`${running.origin}${feedPath}`);
  const feedPage = await decodeResponse(TransitionFeedResponse, feedResponse);
  check(
    "the transition feed serves arrival order over HTTP",
    feedResponse.status === 200 &&
      feedPage.order === "arrival" &&
      feedPage.upToDate &&
      feedPage.events.some((event) => event.issueId === "smoke-later"),
    feedPage.events.map((event) => `${event.change}:${event.issueId}`),
  );

  // The feed's own cursor must replay exactly what was appended after it.
  const feedTail = await requestJson(
    TransitionFeedResponse,
    `${running.origin}${feedPath}?offset=${encodeURIComponent(feedPage.nextOffset)}`,
  );
  check("resuming the feed at its tail replays nothing", feedTail.events.length === 0, feedTail);

  const refusedFeed = await request(`${running.origin}${feedPath}?offset=not-an-offset`);
  const refusedFeedBody = await decodeResponse(FeedResumeError, refusedFeed);
  check(
    "an unusable feed offset declares replay-from-start",
    refusedFeed.status === 409 && refusedFeedBody.recovery === "replay-from-start",
    refusedFeed.status,
  );

  // === the document sink's cached workspace summary ===
  const summaryPath = "/document/workspaces/main/summary";
  const summaryResponse = await request(`${running.origin}${summaryPath}`);
  const summaryDocument = await decodeResponse(WorkspaceSummary, summaryResponse.clone());
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

  const conditional = await request(`${running.origin}${summaryPath}`, {
    headers: { "if-none-match": summaryEtag },
  });
  check(
    "a conditional summary request is a bodiless 304",
    conditional.status === 304 && (await conditional.text()) === "",
    conditional.status,
  );

  // === a second workspace in the same host ===
  const opsSeedResponse = await post(running.origin, "/api/workspaces/ops/seed", {});
  const opsSeed = await decodeResponse(SeedResponse, opsSeedResponse);
  check(
    "a second workspace seeds in the same host",
    opsSeed.seeded && opsSeed.issues.length === 4,
    opsSeed,
  );

  const opsIssues = await requestJson(
    IssuesResponse,
    `${running.origin}/api/workspaces/ops/issues`,
  );
  check(
    "the second workspace holds only its own rows",
    opsIssues.rows.length === 4 && !opsIssues.rows.some((row) => row.issueId === "smoke-issue"),
    opsIssues.rows.map((row) => row.issueId),
  );

  const opsSummary = await requestJson(
    WorkspaceSummary,
    `${running.origin}/document/workspaces/ops/summary`,
  );
  check(
    "each workspace's document sink is served from its own partition",
    opsSummary.workspaceId === "ops" && opsSummary.issues.total === 4,
    opsSummary,
  );

  const metrics = await requestJson(HostMetricsResponse, `${running.origin}/host/metrics`);
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
  const seededLabels = await requestJson(
    LabelCountsResponse,
    `${running.origin}/api/workspaces/main/label-counts`,
  );
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

  const attachedResponse = await post(
    running.origin,
    "/api/workspaces/main/issues/seed-plan/labels",
    {
      commandId: "smoke-attach",
      labelId: "bug",
    },
  );
  const attached = await decodeResponse(LabelCommandResponse, attachedResponse);
  check(
    "attaching a label appends a membership fact",
    attached.membershipId === "seed-plan.bug" && attached.attached && !attached.reconciled,
    attached,
  );
  await Bun.sleep(200);
  const liveCounts = [...countsConnection.db.collections.labelCounts.entries()].map(([, r]) => r);
  check(
    "the attach reaches the live consumer without a refresh",
    countOf(liveCounts, "bug") === 2,
    liveCounts,
  );

  const retriedAttachResponse = await post(
    running.origin,
    "/api/workspaces/main/issues/seed-plan/labels",
    {
      commandId: "smoke-attach",
      labelId: "bug",
    },
  );
  const retriedAttach = await decodeResponse(LabelCommandResponse, retriedAttachResponse);
  check("a retried membership command appends nothing", retriedAttach.reconciled, retriedAttach);

  const detachedResponse = await post(
    running.origin,
    "/api/workspaces/main/issues/seed-plan/labels/detach",
    {
      commandId: "smoke-detach",
      labelId: "bug",
    },
  );
  const detached = await decodeResponse(LabelCommandResponse, detachedResponse);
  const afterDetach = await requestJson(
    LabelCountsResponse,
    `${running.origin}/api/workspaces/main/label-counts`,
  );
  check(
    "detaching removes the count with no State delete",
    !detached.attached && countOf(afterDetach.rows, "bug") === 1,
    afterDetach,
  );
  const memberships = await requestJson(
    IssueLabelsResponse,
    `${running.origin}/api/workspaces/main/issue-labels`,
  );
  check(
    "the detached membership stays in the relation, marked detached",
    memberships.rows.find((row) => row.membershipId === "seed-plan.bug")?.attached === false,
    memberships.rows,
  );
  countsConnection.close();

  const bothSinks = await requestJson(
    SinkSessionResponse,
    `${running.origin}/api/workspaces/main/sink-session`,
  );
  check(
    "the sink session names both checked State contracts",
    bothSinks.labelCounts.sink === "issue-tracker.board-label-counts" &&
      bothSinks.labelCounts.contractFingerprint === seededLabels.contractFingerprint,
    bothSinks,
  );

  // === the cross-domain exchange: two workspaces into one user inbox ===
  const opsAssignedResponse = await post(
    running.origin,
    "/api/workspaces/ops/issues/seed-plan/assignee",
    {
      commandId: "smoke-assign-ops",
      assigneeId: "ada",
    },
  );
  const opsAssigned = await decodeResponse(CommandResponse, opsAssignedResponse);
  check("the second workspace accepts its own assignment", opsAssigned.row?.assigneeId === "ada");

  const exchanged = await running.host.host.exchange();
  check(
    "one exchange pass reads every open workspace",
    exchanged.length === 2 && exchanged.every((report) => !report.failed),
    exchanged,
  );

  const inbox = await requestJson(InboxResponse, `${running.origin}/api/users/ada/inbox`);
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

  const cursors = await requestJson(
    ExchangeStatusResponse,
    `${running.origin}/api/global/exchange`,
  );
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

  const afterRestart = await requestJson(
    IssuesResponse,
    `${running.origin}/api/workspaces/main/issues`,
  );
  check(
    "a restart preserves the maintained rows",
    afterRestart.rows.length === 8 &&
      afterRestart.rows.find((row) => row.issueId === "smoke-issue")?.status === "done" &&
      afterRestart.rows.find((row) => row.issueId === "smoke-issue")?.assigneeId === "ada",
    afterRestart.rows.map((row) => row.issueId),
  );

  const afterRestartRetry = await post(running.origin, "/api/workspaces/main/issues", createBody);
  const afterRestartBody = await decodeResponse(CommandResponse, afterRestartRetry);
  const notificationsAfterRestart = await requestJson(
    NotificationsResponse,
    `${running.origin}/api/workspaces/main/notifications`,
  );
  check(
    "the outbox survives the restart with its settled state intact",
    notificationsAfterRestart.pending === 0 && notificationsAfterRestart.delivered === 1,
    notificationsAfterRestart,
  );
  const redrainedAfterRestartResponse = await post(
    running.origin,
    "/api/workspaces/main/notifications/drain",
    {},
  );
  const redrainedAfterRestart = await decodeResponse(DrainResponse, redrainedAfterRestartResponse);
  check(
    "a restarted host re-delivers nothing that was already delivered",
    redrainedAfterRestart.claimed === 0,
    redrainedAfterRestart,
  );

  const inboxAfterRestart = await requestJson(
    InboxResponse,
    `${running.origin}/api/users/ada/inbox`,
  );
  check(
    "the user inbox survives the restart",
    inboxAfterRestart.rows.length === 2,
    inboxAfterRestart,
  );

  await request(`${running.origin}/api/workspaces/ops/issues`);
  const exchangedAfterRestart = await running.host.host.exchange();
  check(
    "a restarted host re-exchanges nothing that was already applied",
    exchangedAfterRestart.length === 2 &&
      exchangedAfterRestart.every((report) => !report.failed && report.applied === 0),
    exchangedAfterRestart,
  );

  const labelsAfterRestart = await requestJson(
    LabelCountsResponse,
    `${running.origin}/api/workspaces/main/label-counts`,
  );
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
  await running.host.host.sweepIdle(DateTime.toEpochMillis(DateTime.nowUnsafe()) + 3_600_000);
  check(
    "every partition was given up before the cold pass",
    running.host.host.openPartitions().length === 0,
    running.host.host.openPartitions(),
  );
  const coldPass = await running.host.host.exchange();
  const graceInbox = await requestJson(InboxResponse, `${running.origin}/api/users/grace/inbox`);
  check(
    "a closed workspace is still exchanged into its assignee's inbox",
    coldPass.some((report) => report.source.id === "main" && report.applied === 1) &&
      graceInbox.rows.some((row) => row.issueId === "seed-scale"),
    { coldPass, graceInbox },
  );

  const registered = await requestJson(
    SourceRegistryResponse,
    `${running.origin}/api/global/sources`,
  );
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

  // oxlint-disable-next-line effecttsgo/global-console -- The executable's final stdout contract reports the preserved 48-check count.
  console.log(`\n${checks.length} checks passed`);
} finally {
  await stop(running);
}
