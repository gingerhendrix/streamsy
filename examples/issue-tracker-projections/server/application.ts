/**
 * Host-independent application: streams service layer, command workflows, and
 * the HTTP router. Every operation returns an Effect; only a host converts one
 * into a Promise.
 */
import type { JsonValue, StreamProtocolClient } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import type { SourceAck } from "@streamsy/experimental/causal";
import { AppendStreamsLive, ReadStreams, ReadStreamsLive } from "@streamsy/experimental/effect";
import {
  DerivedRecoveryLive,
  DerivedStateHistoryLive,
  FanInRecoveryLive,
} from "@streamsy/experimental/ivm-mesh";
import { Effect, Layer } from "effect";
import type {
  BoardResponse,
  CoverageReport,
  CreateIssueRequest,
  CreateProjectRequest,
  HealthResponse,
  IssueCommandRequest,
  MutationResponse,
  ProjectsResponse,
} from "../shared/api.ts";
import {
  assertIdentifier,
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  isKnownMember,
  PROJECT_COLLECTION,
  streamNames,
  TEAM,
  type IssueDetail,
  type IssueEvent,
  type IssuePriority,
  type IssueStatus,
  type Project,
} from "../shared/domain.ts";
import { ensureStream, LaneRegistry, SCHEMA_VERSION } from "./bindings.ts";
import {
  appendIssueEvent,
  appendMembershipFact,
  commandProducer,
  membershipCommandId,
} from "./commands.ts";
import {
  projectionContext,
  proveChain,
  readBoard,
  runIssueDetail,
  runProjectBoard,
  type ChainProbe,
  type ProjectionContext,
} from "./projections.ts";

/** All mesh capabilities the application needs, wired over the fixed client. */
export const MeshLayer = Layer.mergeAll(
  DerivedRecoveryLive,
  DerivedStateHistoryLive,
  FanInRecoveryLive,
).pipe(Layer.provideMerge(Layer.mergeAll(ReadStreamsLive, AppendStreamsLive)));

export type MeshServices = Layer.Success<typeof MeshLayer>;

export interface ApplicationOptions {
  readonly client: StreamProtocolClient;
  readonly host: "local" | "cloudflare";
  readonly deployment: string;
  /** Serve the static shell; the browser build fills this in. */
  readonly assets?: (request: Request) => Promise<Response | undefined>;
  /** Optional best-effort wake for background board catch-up. */
  readonly wake?: (message: WakeMessage) => Promise<void>;
}

export interface WakeMessage {
  readonly workspaceId: string;
  readonly projectId: string;
  readonly issueId?: string;
}

const lanes = new LaneRegistry();

function context(options: ApplicationOptions, workspaceId: string): ProjectionContext {
  return projectionContext(options.client, assertIdentifier(workspaceId, "workspaceId"), lanes);
}

const readAllItems = Effect.fn("Application.readAllItems")(function* (binding: StreamBinding) {
  const reads = yield* ReadStreams;
  const opened = yield* reads.open(binding, { live: false });
  if (opened.status !== "ok") return { status: opened.status } as const;
  const items: JsonValue[] = [];
  while (true) {
    const next = yield* opened.session.next;
    if (next.done) break;
    if (next.value.kind !== "json") throw new TypeError(`${binding.streamId} must be JSON`);
    items.push(...next.value.items);
  }
  const ended = yield* opened.session.done;
  if (ended.status === "cancelled") return yield* Effect.interrupt;
  return { status: "ok" as const, items: items as readonly JsonValue[] };
});

const readAllScoped = (binding: StreamBinding) => readAllItems(binding).pipe(Effect.scoped);

export const listProjects = Effect.fn("Application.listProjects")(function* (
  options: ApplicationOptions,
  workspaceId: string,
) {
  const ctx = context(options, workspaceId);
  const read = yield* readAllScoped(ctx.bindings.projects(workspaceId));
  if (read.status !== "ok") return [] as readonly Project[];
  const projects = new Map<string, Project>();
  for (const item of read.items) {
    if (!isRecord(item) || item.type !== PROJECT_COLLECTION || typeof item.key !== "string")
      continue;
    if (isRecord(item.headers) && item.headers.operation === "delete") {
      projects.delete(item.key);
      continue;
    }
    projects.set(item.key, item.value as unknown as Project);
  }
  return Array.from(projects.values());
});

export const createProject = Effect.fn("Application.createProject")(function* (
  options: ApplicationOptions,
  workspaceId: string,
  request: CreateProjectRequest,
) {
  assertIdentifier(workspaceId, "workspaceId");
  assertIdentifier(request.projectId, "projectId");
  assertIdentifier(request.projectKey, "projectKey");
  if (request.name.trim().length === 0) throw new TypeError("Project name is required");
  yield* Effect.promise(async () => {
    await ensureStream(options.client, streamNames.projects(workspaceId));
    await ensureStream(options.client, streamNames.membership(workspaceId, request.projectId));
    await ensureStream(options.client, streamNames.board(workspaceId, request.projectId));
  });
  const project: Project = {
    projectId: request.projectId,
    projectKey: request.projectKey.toUpperCase(),
    name: request.name,
  };
  // Project rows are application-owned State, appended idempotently by key.
  const producer = yield* Effect.promise(() =>
    commandProducer(`project:${workspaceId}:${request.projectId}`),
  );
  yield* Effect.promise(() =>
    options.client.stream(streamNames.projects(workspaceId)).appendJsonBatch(
      [
        {
          type: PROJECT_COLLECTION,
          key: project.projectId,
          value: project as unknown as JsonValue,
          headers: { operation: "upsert" },
        },
      ],
      { producer },
    ),
  );
  return project;
});

/**
 * Demo-only issue key allocator: the number of durable join facts in the
 * project membership stream. Concurrent creation can repeat a display key; key
 * uniqueness is not a correctness law in this demo.
 */
const nextIssueKey = Effect.fn("Application.nextIssueKey")(function* (
  ctx: ProjectionContext,
  projectId: string,
  projectKey: string,
) {
  const read = yield* readAllScoped(ctx.bindings.membership(ctx.workspaceId, projectId));
  const joins =
    read.status === "ok"
      ? read.items.filter((item) => isRecord(item) && item.type === "IssueJoined").length
      : 0;
  return `${projectKey}-${100 + joins}`;
});

export const createIssue = Effect.fn("Application.createIssue")(function* (
  options: ApplicationOptions,
  workspaceId: string,
  request: CreateIssueRequest,
) {
  const ctx = context(options, workspaceId);
  assertIdentifier(request.issueId, "issueId");
  assertIdentifier(request.projectId, "projectId");
  if (request.title.trim().length === 0) throw new TypeError("Issue title is required");
  const projects = yield* listProjects(options, workspaceId);
  const project = projects.find((candidate) => candidate.projectId === request.projectId);
  if (project === undefined) return { status: "unknown-project" as const };

  yield* Effect.promise(async () => {
    await ensureStream(options.client, streamNames.issueEvents(workspaceId, request.issueId));
    await ensureStream(options.client, streamNames.issueDetail(workspaceId, request.issueId));
    await ensureStream(options.client, streamNames.membership(workspaceId, request.projectId));
    await ensureStream(options.client, streamNames.board(workspaceId, request.projectId));
  });

  const issueKey = yield* nextIssueKey(ctx, request.projectId, project.projectKey);
  const at = new Date().toISOString();
  const event: IssueEvent = {
    type: "IssueCreated",
    commandId: request.commandId,
    at,
    issueId: request.issueId,
    issueKey,
    projectId: request.projectId,
    title: request.title,
    status: validStatus(request.status ?? "backlog"),
    priority: validPriority(request.priority ?? "medium"),
    creatorId: validMember(request.creatorId ?? TEAM[0].id),
  };

  const producer = yield* Effect.promise(() => commandProducer(request.commandId));
  const appended = yield* appendIssueEvent(
    ctx.bindings.issueEvents(workspaceId, request.issueId),
    event,
    producer,
  );
  if (appended.status === "rejected") return { status: "rejected" as const, appended };

  // Step two of the creation workflow. Its deterministic producer id makes a
  // repeated repair of a partly complete creation safe.
  const joinProducer = yield* Effect.promise(() =>
    commandProducer(membershipCommandId(request.commandId, request.issueId)),
  );
  const joined = yield* appendMembershipFact(
    ctx.bindings.membership(workspaceId, request.projectId),
    { type: "IssueJoined", issueId: request.issueId, from: null },
    joinProducer,
  );
  if (joined.status === "rejected") return { status: "rejected" as const, appended: joined };

  return yield* settle(options, ctx, {
    issueId: request.issueId,
    projectId: request.projectId,
    commandId: request.commandId,
    ack: appended.ack,
    reconciled: appended.status === "reconciled",
  });
});

export const issueCommand = Effect.fn("Application.issueCommand")(function* (
  options: ApplicationOptions,
  workspaceId: string,
  issueId: string,
  request: IssueCommandRequest,
) {
  const ctx = context(options, workspaceId);
  assertIdentifier(issueId, "issueId");
  const detail = yield* loadDetail(ctx, issueId);
  if (detail === undefined) return { status: "unknown-issue" as const };

  const at = new Date().toISOString();
  const event = buildEvent(request, at);
  const producer = yield* Effect.promise(() => commandProducer(request.commandId));
  const appended = yield* appendIssueEvent(
    ctx.bindings.issueEvents(workspaceId, issueId),
    event,
    producer,
  );
  if (appended.status === "rejected") return { status: "rejected" as const, appended };

  return yield* settle(options, ctx, {
    issueId,
    projectId: detail.projectId,
    commandId: request.commandId,
    ack: appended.ack,
    reconciled: appended.status === "reconciled",
  });
});

/**
 * Run the affected projections immediately for low latency, then prove the
 * chain. Immediate work is an optimisation: a lost pass converges through the
 * wake consumer or the explicit repair endpoint.
 */
const settle = Effect.fn("Application.settle")(function* (
  options: ApplicationOptions,
  ctx: ProjectionContext,
  input: {
    readonly issueId: string;
    readonly projectId: string;
    readonly commandId: string;
    readonly ack: SourceAck;
    readonly reconciled: boolean;
  },
) {
  yield* runIssueDetail(ctx, input.issueId);
  yield* runProjectBoard(ctx, input.projectId);
  const probe = yield* proveChain(ctx, {
    issueId: input.issueId,
    projectId: input.projectId,
    ack: input.ack,
  });
  const detail = yield* loadDetail(ctx, input.issueId);

  if (options.wake && probe.coverage.status !== "proven") {
    yield* Effect.promise(() =>
      options.wake!({
        workspaceId: ctx.workspaceId,
        projectId: input.projectId,
        issueId: input.issueId,
      }).catch(() => undefined),
    );
  }

  const response: MutationResponse = {
    commandId: input.commandId,
    issueId: input.issueId,
    projectId: input.projectId,
    ack: { stream: input.ack.identity.name, position: input.ack.position },
    reconciled: input.reconciled,
    coverage: coverageReport(ctx, input, probe),
    detail: detail ?? null,
  };
  return { status: "ok" as const, response };
});

function coverageReport(
  ctx: ProjectionContext,
  input: { readonly issueId: string; readonly projectId: string; readonly ack: SourceAck },
  probe: ChainProbe,
): CoverageReport {
  return {
    status: probe.coverage.status,
    ...(probe.coverage.status === "proven" ? {} : { blockedAt: probe.coverage.blockedAt }),
    ack: { stream: input.ack.identity.name, position: input.ack.position },
    hops: [
      {
        label: "issue-detail",
        source: streamNames.issueEvents(ctx.workspaceId, input.issueId),
        through: probe.detail.through,
        output: probe.detail.output,
      },
      {
        label: "project-board",
        source: streamNames.issueDetail(ctx.workspaceId, input.issueId),
        through: probe.board.through,
        output: probe.board.output,
      },
    ],
  };
}

export const loadDetail = Effect.fn("Application.loadDetail")(function* (
  ctx: ProjectionContext,
  issueId: string,
) {
  const read = yield* readAllScoped(ctx.bindings.issueDetail(ctx.workspaceId, issueId));
  if (read.status !== "ok") return undefined;
  let detail: IssueDetail | undefined;
  for (const item of read.items) {
    if (!isRecord(item) || item.type !== "issue-detail") continue;
    if (isRecord(item.headers) && item.headers.operation === "delete") {
      detail = undefined;
      continue;
    }
    detail = item.value as unknown as IssueDetail;
  }
  return detail;
});

/** Bounded repair: catch up every active member and then the board itself. */
export const repairProject = Effect.fn("Application.repairProject")(function* (
  options: ApplicationOptions,
  workspaceId: string,
  projectId: string,
) {
  const ctx = context(options, workspaceId);
  assertIdentifier(projectId, "projectId");
  const membership = yield* readAllScoped(ctx.bindings.membership(workspaceId, projectId));
  const active = new Set<string>();
  if (membership.status === "ok") {
    for (const item of membership.items) {
      if (!isRecord(item) || typeof item.issueId !== "string") continue;
      if (item.type === "IssueJoined") active.add(item.issueId);
      if (item.type === "IssueLeft") active.delete(item.issueId);
    }
  }
  const members = Array.from(active).toSorted();
  for (const issueId of members) {
    yield* runIssueDetail(ctx, issueId);
  }
  const board = yield* runProjectBoard(ctx, projectId);
  return { repaired: members, board: board.status };
});

function buildEvent(request: IssueCommandRequest, at: string): IssueEvent {
  const base = { commandId: request.commandId, at };
  switch (request.type) {
    case "rename":
      if (request.title.trim().length === 0) throw new TypeError("title is required");
      return { ...base, type: "IssueRenamed", title: request.title };
    case "status":
      return { ...base, type: "IssueStatusChanged", status: validStatus(request.status) };
    case "priority":
      return { ...base, type: "IssuePriorityChanged", priority: validPriority(request.priority) };
    case "assign":
      return {
        ...base,
        type: "IssueAssigned",
        assigneeId: request.assigneeId === null ? null : validMember(request.assigneeId),
      };
    case "comment":
      if (request.body.trim().length === 0) throw new TypeError("comment body is required");
      return {
        ...base,
        type: "CommentAdded",
        commentId: assertIdentifier(request.commentId, "commentId"),
        authorId: validMember(request.authorId),
        body: request.body,
      };
  }
}

function validStatus(value: string): IssueStatus {
  const found = ISSUE_STATUSES.find((status) => status === value);
  if (found === undefined) throw new TypeError(`Unknown issue status ${value}`);
  return found;
}

function validPriority(value: string): IssuePriority {
  const found = ISSUE_PRIORITIES.find((priority) => priority === value);
  if (found === undefined) throw new TypeError(`Unknown issue priority ${value}`);
  return found;
}

function validMember(value: string): string {
  if (!isKnownMember(value)) throw new TypeError(`Unknown team member ${value}`);
  return value;
}

export const getBoard = Effect.fn("Application.getBoard")(function* (
  options: ApplicationOptions,
  workspaceId: string,
  projectId: string,
) {
  const ctx = context(options, workspaceId);
  const board = yield* readBoard(ctx, assertIdentifier(projectId, "projectId"));
  if (board.status !== "ready") return { status: board.status } as const;
  const response: BoardResponse = {
    projectId,
    boardStream: board.boardStream,
    rows: board.rows.toSorted((left, right) => left.issueKey.localeCompare(right.issueKey)),
  };
  return { status: "ok" as const, response };
});

export function health(options: ApplicationOptions): HealthResponse {
  return {
    status: "ok",
    deployment: options.deployment,
    schemaVersion: SCHEMA_VERSION,
    host: options.host,
  };
}

export function projectsResponse(
  workspaceId: string,
  projects: readonly Project[],
): ProjectsResponse {
  return { workspaceId, projects };
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
