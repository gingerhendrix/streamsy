/**
 * The whole local application, from recorded and live inputs.
 *
 * This is the Integration 2 acceptance script. It is not another smoke test of
 * one surface: it assembles every product the tracker has across all three
 * domains and asserts they agree with each other.
 *
 * **Recorded inputs** come first, and they are the more interesting half. The
 * `acme` workspace is replayed by appending canonical facts *directly to the
 * durable streams*, exactly as a producer wrote them — no command path, no
 * receipts, no in-process state. Every product must then be derivable from
 * those facts alone. The recording deliberately contains a fact whose domain
 * `sequence` is out of order relative to its arrival, so the arrival-order law
 * is exercised by the product rather than only by a unit test.
 *
 * **Live inputs** follow: commands over HTTP into a second workspace, so both
 * halves of the same host are checked, and so the cross-workspace inbox has two
 * sources to reconcile.
 *
 * Finally the whole host is restarted and every claim is re-checked. Nothing is
 * asserted from a command response: every check reads a published product or a
 * maintained read model back over the network.
 */
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This executable reads its checked-in recording and creates an isolated native temporary database directory.
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
// oxlint-disable-next-line effecttsgo/node-builtin-import -- This executable resolves its checked-in recording path and isolated database directory through Bun's Node-compatible path API.
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import { createLocalHost, type LocalHostOptions } from "../server/local.ts";
import { streamNames } from "../domain/declaration.ts";
import { WorkspaceSummary } from "../domain/issue.ts";
import {
  DrainResponse,
  InboxResponse,
  IssuesResponse,
  LabelCommandResponse,
  LabelCountsResponse,
  TransitionFeedResponse,
} from "../shared/api.ts";
import { createBoardConnection } from "../src/lib/board-db.ts";
import { createLabelCountsConnection } from "../src/lib/label-counts-db.ts";
import { decodeResponse, request } from "./http.ts";

const checks: string[] = [];
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This executable assertion boundary accepts already-decoded values from several contracts solely to preserve their native stderr diagnostics on failure.
function check(label: string, condition: boolean, detail?: unknown): void {
  if (!condition) {
    // oxlint-disable-next-line effecttsgo/global-console -- A failed acceptance check must retain its stderr diagnostic before the executable exits non-zero.
    console.error(`FAIL ${label}`, detail === undefined ? "" : detail);
    process.exit(1);
  }
  checks.push(label);
  // oxlint-disable-next-line effecttsgo/global-console -- Per-check stdout is the acceptance script's documented terminal result.
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

const post = (origin: string, path: string, body: Schema.Json): Promise<Response> =>
  request(`${origin}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

// oxlint-disable-next-line effecttsgo/async-function -- This executable HTTP assertion boundary must inspect a non-success body's native Response text before decoding successful JSON.
const get = async <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  origin: string,
  path: string,
): Promise<S["Type"]> => {
  const response = await request(`${origin}${path}`, { headers: { accept: "application/json" } });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${path} → ${response.status} ${text.slice(0, 300)}`);
  }
  return decodeResponse(schema, response);
};

/**
 * The recording's own shape.
 *
 * Rows and facts are typed as JSON values rather than as the domain schemas
 * they will become, because that is what a recording *is*: bytes a producer
 * wrote. The host decodes them through the declared schemas on the way in, and
 * a recording that no longer decodes must fail there rather than here.
 */
const RecordedValue = Schema.Record(
  Schema.String,
  Schema.Union([Schema.String, Schema.Finite, Schema.Boolean, Schema.Null]),
);

const Recording = Schema.Struct({
  workspaceId: Schema.String,
  catalog: Schema.Record(Schema.String, Schema.Array(RecordedValue)),
  issueEvents: Schema.Array(RecordedValue),
  issueLabelEvents: Schema.Array(RecordedValue),
});

/** Which field of a recorded catalog row is that collection's declared key. */
function catalogKey(collection: string): string | undefined {
  switch (collection) {
    case "projects":
      return "projectId";
    case "users":
      return "userId";
    case "labels":
      return "labelId";
    default:
      return undefined;
  }
}

const here = dirname(fileURLToPath(import.meta.url));
const recording = Schema.decodeSync(Schema.fromJsonString(Recording))(
  readFileSync(join(here, "recordings/acme.json"), "utf8"),
);

/**
 * Replay a recording onto the durable streams, bypassing the command path.
 *
 * Facts are appended one at a time and in file order, so the log's arrival
 * order is the recording's order — which is what makes the out-of-sequence
 * entry meaningful.
 */
// oxlint-disable-next-line effecttsgo/async-function -- Recording replay deliberately sequences native durable-stream appends so arrival order remains the checked file order.
async function replay(running: Running): Promise<void> {
  const workspaceId = recording.workspaceId;
  // One request opens the partition and creates every stream this workspace owns.
  await get(IssuesResponse, running.origin, `/api/workspaces/${workspaceId}/issues`);
  const client = running.host.host.partition({ kind: "workspace", id: workspaceId });
  if ("_tag" in client) {
    const { _tag: tag } = client;
    throw new Error(`cannot open ${workspaceId}: ${tag}`);
  }

  for (const [collection, rows] of Object.entries(recording.catalog)) {
    for (const row of rows) {
      const keyField = catalogKey(collection);
      if (keyField === undefined) throw new Error(`unknown recorded collection ${collection}`);
      await post(running.origin, `/api/workspaces/${workspaceId}/catalog/${collection}`, {
        key: String(row[keyField]),
        value: row,
      });
    }
  }
  for (const event of recording.issueEvents) {
    const appended = await client.client
      .stream(streamNames.issueEvents(workspaceId))
      .append(JSON.stringify(event), { contentType: "application/json" });
    if (appended.status !== "appended") throw new Error(`replay failed: ${appended.status}`);
  }
  for (const event of recording.issueLabelEvents) {
    const appended = await client.client
      .stream(streamNames.issueLabelEvents(workspaceId))
      .append(JSON.stringify(event), { contentType: "application/json" });
    if (appended.status !== "appended") throw new Error(`replay failed: ${appended.status}`);
  }
}

interface LabelCountBody {
  readonly labelId: string;
  readonly issueCount: number;
}

const countOf = (rows: readonly LabelCountBody[], labelId: string): number =>
  rows.find((row) => row.labelId === labelId)?.issueCount ?? 0;

const directory = mkdtempSync(join(tmpdir(), "issue-tracker-full-app-"));
let running = start({ databaseDirectory: directory });

try {
  // ===== recorded inputs =====
  await replay(running);

  const recorded = await get(IssuesResponse, running.origin, "/api/workspaces/acme/issues");
  check(
    "the recorded workspace folds into maintained rows",
    recorded.rows.length === 2 &&
      recorded.rows.find((row) => row.issueId === "acme-2")?.assigneeId === "grace",
    recorded.rows,
  );
  check(
    "arrival order wins over domain sequence in the recorded fold",
    // `r-4` (sequence 3, done) arrived *before* `r-3` (sequence 2, in_progress),
    // so the fold's last word on `acme-1` is the one that landed last.
    recorded.rows.find((row) => row.issueId === "acme-1")?.status === "in_progress",
    recorded.rows,
  );

  const recordedFeed = await get(
    TransitionFeedResponse,
    running.origin,
    "/feed/workspaces/acme/issue-transitions",
  );
  check(
    "the transition feed carries the recording's coalesced changes, in arrival order",
    // One pass folded the whole recording, so the feed carries one change per
    // key rather than one per fact — and the change it carries is the state the
    // *last arriving* fact produced, not the highest-sequence one.
    recordedFeed.order === "arrival" &&
      recordedFeed.events
        .map((event) => `${event.issueId}:${event.change}:${event.status}`)
        .join(",") === "acme-1:enter:in_progress,acme-2:enter:todo",
    recordedFeed.events,
  );

  const recordedCounts = await get(
    LabelCountsResponse,
    running.origin,
    "/api/workspaces/acme/label-counts",
  );
  check(
    "recorded membership facts produce live label counts",
    // `docs` was attached to both issues and then detached from `acme-1`.
    countOf(recordedCounts.rows, "docs") === 1 && countOf(recordedCounts.rows, "bug") === 1,
    recordedCounts.rows,
  );

  const recordedSummary = await get(
    WorkspaceSummary,
    running.origin,
    "/document/workspaces/acme/summary",
  );
  check(
    "the summary document agrees with the maintained rows it is derived from",
    recordedSummary.issues.total === recorded.rows.length &&
      recordedSummary.issues.byStatus.in_progress === 1 &&
      recordedSummary.catalog.labels === 2 &&
      recordedSummary.catalog.users === 2,
    recordedSummary,
  );

  // ===== live inputs =====
  await post(running.origin, "/api/workspaces/live/seed", {});
  await post(running.origin, "/api/workspaces/live/issues", {
    commandId: "live-create",
    issueId: "live-1",
    projectId: "streamsy",
    title: "Live: assemble the application",
    status: "todo",
  });
  await post(running.origin, "/api/workspaces/live/issues/live-1/assignee", {
    commandId: "live-assign",
    assigneeId: "grace",
  });
  await post(running.origin, "/api/workspaces/live/issues/live-1/labels", {
    commandId: "live-attach",
    labelId: "bug",
  });

  const liveCounts = await get(
    LabelCountsResponse,
    running.origin,
    "/api/workspaces/live/label-counts",
  );
  check(
    "live commands move the live workspace's label counts",
    countOf(liveCounts.rows, "bug") === 2,
    liveCounts.rows,
  );
  check(
    "the recorded workspace's counts are untouched by the live one",
    countOf(
      (await get(LabelCountsResponse, running.origin, "/api/workspaces/acme/label-counts")).rows,
      "bug",
    ) === 1,
  );

  const drainedResponse = await post(
    running.origin,
    "/api/workspaces/live/notifications/drain",
    {},
  );
  const drained = await decodeResponse(DrainResponse, drainedResponse);
  check("the assignment notification is delivered once", drained.delivered === 1, drained);

  // ===== the cross-workspace inbox: two domains feeding one user =====
  const passes = await running.host.host.exchange();
  check(
    "one exchange pass covers both the recorded and the live workspace",
    passes.length === 2 && passes.every((pass) => !pass.failed),
    passes,
  );
  const inbox = await get(InboxResponse, running.origin, "/api/users/grace/inbox");
  check(
    "one user inbox is fed by the recorded and the live workspace",
    inbox.rows.length === 2 &&
      inbox.rows
        .map((row) => row.workspaceId)
        .toSorted()
        .join(",") === "acme,live",
    inbox.rows,
  );

  // ===== the published products, bound as a client binds them =====
  const board = createBoardConnection({
    workspaceId: "acme",
    origin: running.origin,
    onStatus: () => undefined,
  });
  const counts = createLabelCountsConnection({
    workspaceId: "acme",
    origin: running.origin,
    onStatus: () => undefined,
  });
  await Promise.all([board.preload(), counts.preload()]);
  const boundIssues = [...board.db.collections.issues.entries()].map(([, row]) => row);
  const boundCounts = [...counts.db.collections.labelCounts.entries()].map(([, row]) => row);
  check(
    "both generated bindings hold the same product the read models report",
    boundIssues.length === recorded.rows.length && countOf(boundCounts, "docs") === 1,
    { boundIssues, boundCounts },
  );
  board.close();
  counts.close();

  // ===== forced restart of the whole host =====
  const beforeRestart = {
    rows: recorded.rows,
    counts: recordedCounts.rows,
    feed: recordedFeed.events,
    inbox: inbox.rows,
  };
  await stop(running);
  running = start({ databaseDirectory: directory });

  check(
    "the recorded workspace's rows survive a whole-host restart",
    JSON.stringify(
      (await get(IssuesResponse, running.origin, "/api/workspaces/acme/issues")).rows,
    ) === JSON.stringify(beforeRestart.rows),
  );
  check(
    "label counts survive a whole-host restart",
    JSON.stringify(
      (await get(LabelCountsResponse, running.origin, "/api/workspaces/acme/label-counts")).rows,
    ) === JSON.stringify(beforeRestart.counts),
  );
  check(
    "the transition feed gains nothing from a restart",
    JSON.stringify(
      (await get(TransitionFeedResponse, running.origin, "/feed/workspaces/acme/issue-transitions"))
        .events,
    ) === JSON.stringify(beforeRestart.feed),
  );
  check(
    "the user inbox survives a whole-host restart",
    JSON.stringify((await get(InboxResponse, running.origin, "/api/users/grace/inbox")).rows) ===
      JSON.stringify(beforeRestart.inbox),
  );

  const afterRestartPasses = await running.host.host.exchange();
  check(
    "a restarted host re-exchanges nothing, from the durable registry",
    afterRestartPasses.length === 2 &&
      afterRestartPasses.every((pass) => !pass.failed && pass.applied === 0),
    afterRestartPasses,
  );

  const retriedResponse = await post(running.origin, "/api/workspaces/live/issues/live-1/labels", {
    commandId: "live-attach",
    labelId: "bug",
  });
  const retried = await decodeResponse(LabelCommandResponse, retriedResponse);
  check("a membership retry still reconciles after the restart", retried.reconciled, retried);

  // oxlint-disable-next-line effecttsgo/global-console -- The executable's final stdout contract reports the preserved acceptance-check count.
  console.log(`\n${checks.length} checks passed`);
} finally {
  await stop(running);
}
