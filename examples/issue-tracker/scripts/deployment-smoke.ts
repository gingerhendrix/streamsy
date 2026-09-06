/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/global-fetch, effecttsgo/global-random, effecttsgo/process-env -- This bounded deployment executable drives the public Worker with Web fetch and reports non-secret evidence. */
import { Schema } from "effect";
import {
  assignmentNotifications,
  boardIssues,
  boardLabelCounts,
  issueTransitions,
  workspaceSummary,
} from "../domain/declaration.ts";
import { WorkspaceSummary } from "../domain/issue.ts";
import { PLAN_HASH } from "../server/config.ts";
import {
  CommandResponse,
  DrainResponse,
  HealthResponse,
  IssuesResponse,
  LabelCommandResponse,
  LabelCountsResponse,
  NotificationsResponse,
  SeedResponse,
  SinkSessionResponse,
  TransitionFeedResponse,
} from "../shared/api.ts";
import {
  GLOBAL_OBJECT_CLASS,
  USER_OBJECT_CLASS,
  WORKSPACE_OBJECT_CLASS,
  WORKSPACE_OBJECT_MIGRATION,
} from "../alchemy.run.ts";

const base = required("DEPLOYMENT_URL").replace(/\/$/, "");
const stage = required("DEPLOYMENT_STAGE");
const workerId = required("DEPLOYMENT_WORKER_ID");
const workerName = required("DEPLOYMENT_WORKER_NAME");
const namespaceId = required("DEPLOYMENT_NAMESPACE_ID");
const userNamespaceId = required("DEPLOYMENT_USER_NAMESPACE_ID");
const globalNamespaceId = required("DEPLOYMENT_GLOBAL_NAMESPACE_ID");
const localVerification = base.startsWith("http://localhost:");
const suffix = (process.env.DEPLOYMENT_EVIDENCE_ID ?? Math.random().toString(36).slice(2, 10))
  .toLowerCase()
  .replace(/[^a-z0-9_-]/g, "")
  .slice(0, 24);
const workspaceId = `smoke-${suffix}`;
const issueId = `issue-${suffix}`;
const createBody = {
  commandId: `create-${suffix}`,
  issueId,
  projectId: "streamsy",
  title: "Integration 3B deployment smoke",
  status: "backlog",
} as const;

const StateMessage = Schema.Struct({
  type: Schema.optionalKey(Schema.String),
  key: Schema.optionalKey(Schema.String),
  value: Schema.optionalKey(
    Schema.Struct({
      status: Schema.optionalKey(Schema.String),
      issueCount: Schema.optionalKey(Schema.Finite),
    }),
  ),
});

const requestId = `smoke-${suffix}-health`;
const healthResponse = await call("GET", "/health", undefined, { "x-request-id": requestId });
assert(
  localVerification || healthResponse.headers.get("x-request-id") === requestId,
  "deployed health must preserve request id",
);
const health = await decode(healthResponse, HealthResponse);
assert(health.status === "ok", "health must report ok");
assert(health.deployment === stage, `health deployment must be ${stage}`);
assert(health.planHash === PLAN_HASH, `health plan hash must be ${PLAN_HASH}`);

const shell = await call("GET", "/");
assert(shell.status === 200, `SPA shell must load, got ${shell.status}`);
assert((await shell.text()).includes('id="root"'), "SPA shell must contain the root mount");

const seeded = await decode(
  await call("POST", `/api/workspaces/${workspaceId}/seed`),
  SeedResponse,
);
assert(seeded.seeded, "fresh smoke workspace must seed");
assert(seeded.issues.length === 4, "seed must create four issues");

const createdResponse = await call("POST", `/api/workspaces/${workspaceId}/issues`, createBody);
assert(
  createdResponse.status === 201,
  `first create must return 201, got ${createdResponse.status}`,
);
const created = await decode(createdResponse, CommandResponse);
assert(!created.reconciled, "first create must not reconcile");
assert(created.row?.status === "backlog", "created row must be backlog");

const duplicate = await decode(
  await call("POST", `/api/workspaces/${workspaceId}/issues`, createBody),
  CommandResponse,
);
assert(duplicate.reconciled, "duplicate create must reconcile");
assert(duplicate.ack.offset === created.ack.offset, "duplicate must return original offset");
assert(duplicate.eventId === created.eventId, "duplicate must return original event id");

const sessionBeforeMove = await decode(
  await call("GET", `/api/workspaces/${workspaceId}/sink-session`),
  SinkSessionResponse,
);
const boardBeforeMove = await state(
  `/state/workspaces/${workspaceId}/issues?offset=${encodeURIComponent(sessionBeforeMove.offset)}`,
);
assert(boardBeforeMove.messages.length === 0, "board tail must initially be empty");

const transitionsBeforeMove = await decode(
  await call("GET", `/feed/workspaces/${workspaceId}/issue-transitions`),
  TransitionFeedResponse,
);

const moved = await decode(
  await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/status`, {
    commandId: `move-${suffix}`,
    status: "done",
  }),
  CommandResponse,
);
assert(moved.row?.status === "done", "move must maintain the issue as done");

const boardSuffix = await state(
  `/state/workspaces/${workspaceId}/issues?offset=${encodeURIComponent(sessionBeforeMove.offset)}`,
);
assert(
  boardSuffix.messages.some(
    (message) =>
      message.type === "issue" && message.key === issueId && message.value?.status === "done",
  ),
  "board native resume must contain the moved issue",
);
assert(boardSuffix.offset !== sessionBeforeMove.offset, "board native position must advance");

const transitionSuffix = await decode(
  await call(
    "GET",
    `/feed/workspaces/${workspaceId}/issue-transitions?offset=${encodeURIComponent(
      transitionsBeforeMove.nextOffset,
    )}`,
  ),
  TransitionFeedResponse,
);
assert(
  transitionSuffix.events.some(
    (event) => event.issueId === issueId && event.status === "done" && event.change === "update",
  ),
  "transition native resume must contain the status move",
);
assert(
  transitionSuffix.nextOffset !== transitionsBeforeMove.nextOffset,
  "transition native position must advance",
);

const labelsBefore = await decode(
  await call("GET", `/api/workspaces/${workspaceId}/label-counts`),
  LabelCountsResponse,
);
const bugBefore = labelsBefore.rows.find((row) => row.labelId === "bug")?.issueCount;
assert(bugBefore !== undefined, "seeded bug label count must exist");
const labelPositionBefore = (
  await decode(
    await call("GET", `/api/workspaces/${workspaceId}/sink-session`),
    SinkSessionResponse,
  )
).labelCounts.offset;

const attached = await decode(
  await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/labels`, {
    commandId: `attach-${suffix}`,
    labelId: "bug",
  }),
  LabelCommandResponse,
);
assert(attached.attached, "label command must attach bug");
const labelSuffix = await state(
  `/state/workspaces/${workspaceId}/label-counts?offset=${encodeURIComponent(labelPositionBefore)}`,
);
assert(
  labelSuffix.messages.some(
    (message) =>
      message.type === "label-count" &&
      message.key === "bug" &&
      message.value?.issueCount === bugBefore + 1,
  ),
  "label-count native resume must contain the increment",
);
assert(labelSuffix.offset !== labelPositionBefore, "label-count native position must advance");

const detached = await decode(
  await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/labels/detach`, {
    commandId: `detach-${suffix}`,
    labelId: "bug",
  }),
  LabelCommandResponse,
);
assert(!detached.attached, "label command must detach bug");
const labelsAfter = await decode(
  await call("GET", `/api/workspaces/${workspaceId}/label-counts`),
  LabelCountsResponse,
);
assert(
  labelsAfter.rows.find((row) => row.labelId === "bug")?.issueCount === bugBefore,
  "detach must restore the bug label count",
);

await decode(
  await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/assignee`, {
    commandId: `assign-${suffix}`,
    assigneeId: "ada",
  }),
  CommandResponse,
);
const inbox = await eventually(async () => {
  const response = await call("GET", "/api/users/ada/inbox");
  if (!response.ok) return undefined;
  const value = Schema.decodeUnknownSync(
    Schema.Struct({
      userId: Schema.String,
      rows: Schema.Array(
        Schema.Struct({
          inboxId: Schema.String,
          workspaceId: Schema.String,
          issueId: Schema.String,
        }),
      ),
    }),
  )(await response.json());
  return value.rows.some((row) => row.workspaceId === workspaceId && row.issueId === issueId)
    ? value
    : undefined;
});
assert(inbox.userId === "ada", "cross-object inbox must be placed at ada");
const exchange = await decode(
  await call("GET", "/api/global/exchange"),
  Schema.Struct({
    cursors: Schema.Array(
      Schema.Struct({
        exchange: Schema.String,
        source: Schema.Struct({ kind: Schema.String, id: Schema.String }),
        arrival: Schema.Number,
        applied: Schema.Number,
      }),
    ),
  }),
);
assert(
  exchange.cursors.some((cursor) => cursor.source.id === workspaceId && cursor.applied === 1),
  "global cursor must advance once",
);
const sources = await decode(
  await call("GET", "/api/global/sources"),
  Schema.Struct({ sources: Schema.Array(Schema.Struct({ partition: Schema.String })) }),
);
assert(
  sources.sources.some((source) => source.partition === `workspace:${workspaceId}`),
  "workspace source must remain registered",
);
const beforeDrain = await decode(
  await call("GET", `/api/workspaces/${workspaceId}/notifications`),
  NotificationsResponse,
);
assert(beforeDrain.outbox.length === 1, "assignment must create one durable outbox row");
const drain = await decode(
  await call("POST", `/api/workspaces/${workspaceId}/notifications/drain`),
  DrainResponse,
);
const afterDrain = await decode(
  await call("GET", `/api/workspaces/${workspaceId}/notifications`),
  NotificationsResponse,
);
assert(afterDrain.delivered === 1, "notification maintenance/drain must deliver once");
assert(afterDrain.notified.length === 1, "notification target must observe one delivery");
assert(drain.delivered === 0 || drain.delivered === 1, "alarm or explicit drain must own delivery");

const summaryResponse = await call("GET", `/document/workspaces/${workspaceId}/summary`);
assert(summaryResponse.headers.get("etag") !== null, "summary must carry an entity tag");
const summary = await decode(summaryResponse, WorkspaceSummary);
assert(summary.planHash === PLAN_HASH, "summary must carry the accepted plan hash");
assert(summary.issues.total === 5, "summary must include seed rows and smoke issue");
assert(summary.issues.byStatus.done === 2, "summary must include the moved issue in done");

const issues = await decode(
  await call("GET", `/api/workspaces/${workspaceId}/issues`),
  IssuesResponse,
);
assert(issues.rows.find((row) => row.issueId === issueId)?.status === "done", "final row persists");

console.log(
  JSON.stringify(
    {
      stage,
      url: base,
      worker: { id: workerId, name: workerName },
      durableObject: {
        namespaceId,
        binding: "WORKSPACES",
        className: WORKSPACE_OBJECT_CLASS,
        migration: WORKSPACE_OBJECT_MIGRATION,
      },
      userDurableObject: {
        namespaceId: userNamespaceId,
        binding: "USERS",
        className: USER_OBJECT_CLASS,
      },
      globalDurableObject: {
        namespaceId: globalNamespaceId,
        binding: "GLOBALS",
        className: GLOBAL_OBJECT_CLASS,
      },
      workspaceId,
      planHash: PLAN_HASH,
      sinkFingerprints: {
        board: boardIssues.fingerprint,
        labelCounts: boardLabelCounts.fingerprint,
        transitions: issueTransitions.fingerprint,
        summary: workspaceSummary.fingerprint,
        notifications: assignmentNotifications.fingerprint,
      },
      assertions: {
        healthAndRequestId: "passed",
        assets: "passed",
        seedCreateMove: "passed",
        labelAttachDetach: "passed",
        checkedSinkResume: "passed",
        transitionResume: "passed",
        summary: "passed",
        notificationMaintenanceDrain: "passed",
        duplicateOriginalOffset: "passed",
        durableObjectExchange: "passed",
      },
      positions: {
        command: created.ack.offset,
        duplicate: duplicate.ack.offset,
        boardBefore: sessionBeforeMove.offset,
        boardAfter: boardSuffix.offset,
        labelCountsBefore: labelPositionBefore,
        labelCountsAfter: labelSuffix.offset,
        transitionsBefore: transitionsBeforeMove.nextOffset,
        transitionsAfter: transitionSuffix.nextOffset,
      },
    },
    null,
    2,
  ),
);

async function state(path: string): Promise<{
  readonly messages: readonly (typeof StateMessage.Type)[];
  readonly offset: string;
}> {
  const response = await call("GET", path);
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 500)}`);
  const offset = response.headers.get("stream-next-offset");
  if (offset === null) throw new Error(`${path} did not return stream-next-offset`);
  return {
    messages: Schema.decodeUnknownSync(Schema.Array(StateMessage))(JSON.parse(text)),
    offset,
  };
}

function call(
  method: string,
  path: string,
  body?: Readonly<Record<string, string>>,
  headers?: HeadersInit,
): Promise<Response> {
  const requestHeaders = new Headers(headers);
  const init: RequestInit = { method, headers: requestHeaders };
  if (body !== undefined) {
    requestHeaders.set("content-type", "application/json");
    init.body = JSON.stringify(body);
  }
  return fetch(`${base}${path}`, init);
}

async function decode<S extends Schema.ConstraintDecoder<unknown>>(
  response: Response,
  schema: S,
): Promise<S["Type"]> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 500)}`);
  return Schema.decodeUnknownSync(schema)(JSON.parse(text));
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) throw new Error(`${name} is required`);
  return value;
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

async function eventually<A>(read: () => Promise<A | undefined>, timeoutMs = 15_000): Promise<A> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (value !== undefined) return value;
    if (Date.now() >= deadline) throw new Error(`condition not met within ${timeoutMs}ms`);
    await Bun.sleep(250);
  }
}
