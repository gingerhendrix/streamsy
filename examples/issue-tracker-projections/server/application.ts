/**
 * Host-independent application.
 *
 * Every operation is an `Effect` description that declares what it needs in its
 * requirement channel and how it can fail in its error channel. No operation
 * takes an options record, resolves a client, converts a Promise, or throws to
 * signal an expected failure. Only an executable edge — `server/local.ts` or
 * `server/worker.ts` — builds a runtime and runs one of these descriptions.
 */
import type { JsonValue } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { sourceAck, type SourceAck } from "@streamsy/experimental/causal";
import {
  AppendStreams,
  AppendStreamsLive,
  ReadStreams,
  ReadStreamsLive,
} from "@streamsy/experimental/effect";
import {
  DerivedRecoveryLive,
  DerivedStateHistoryLive,
  FanInRecoveryLive,
} from "@streamsy/experimental/ivm-mesh";
import { Effect, Layer, Schema } from "effect";
import type {
  BoardResponse,
  CoverageReport,
  CoverageResponse,
  HealthResponse,
  MutationResponse,
  ProjectionPassReport,
  ProjectsResponse,
  RepairResponse,
} from "../shared/api.ts";
import {
  decodeIssueDetail,
  decodeProject,
  PROJECT_COLLECTION,
  streamNames,
  TEAM,
  type IssueDetail,
  type IssueEvent,
  type Project,
} from "../shared/domain.ts";
import {
  Identifier,
  type CreateIssueRequest,
  type CreateProjectRequest,
  type IssueCommandRequest,
} from "../shared/requests.ts";
import {
  appendIssueEvent,
  appendMembershipFact,
  CommandProducers,
  membershipCommandId,
} from "./commands.ts";
import { AppConfig } from "./config.ts";
import { InvalidRequest, UnknownIssue, UnknownProject } from "./errors.ts";
import { ProjectionLanes } from "./lanes.ts";
import {
  classifyPass,
  faultedPass,
  proveChain,
  readBoard,
  runIssueDetail,
  runProjectBoard,
  type ChainProbe,
} from "./projections.ts";
import { ensureAll, Streams } from "./streams.ts";
import { Wake } from "./wake.ts";

/** All mesh capabilities the application needs, wired over the fixed client. */
export const MeshLayer = Layer.mergeAll(
  DerivedRecoveryLive,
  DerivedStateHistoryLive,
  FanInRecoveryLive,
).pipe(Layer.provideMerge(Layer.mergeAll(ReadStreamsLive, AppendStreamsLive)));

export type MeshServices = Layer.Success<typeof MeshLayer>;

/**
 * Everything an application operation may require.
 *
 * A host builds exactly this set once. `MeshServices` are streaming and
 * recovery capabilities; the rest are owned by this example.
 */
export type ApplicationServices =
  | MeshServices
  | AppConfig
  | Streams
  | ProjectionLanes
  | CommandProducers
  | Wake;

/** A workspace id is a stream path segment, so it is decoded like any other. */
const decodeIdentifier = Schema.decodeUnknownEffect(Identifier);

const identifier = Effect.fnUntraced(function* (field: string, value: string) {
  return yield* decodeIdentifier(value).pipe(
    Effect.mapError(() => InvalidRequest.of(field, "must be one stream path segment")),
  );
});

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
  return { status: "ok" as const, items };
});

const readAllScoped = (binding: StreamBinding) => readAllItems(binding).pipe(Effect.scoped);

export const listProjects = Effect.fn("Application.listProjects")(function* (workspaceId: string) {
  const streams = yield* Streams;
  const read = yield* readAllScoped(streams.bindings.projects(workspaceId));
  if (read.status !== "ok") return [] as readonly Project[];
  const projects = new Map<string, Project>();
  for (const item of read.items) {
    if (!isRecord(item) || item.type !== PROJECT_COLLECTION || typeof item.key !== "string")
      continue;
    if (isRecord(item.headers) && item.headers.operation === "delete") {
      projects.delete(item.key);
      continue;
    }
    projects.set(item.key, decodeProject(item.value));
  }
  return Array.from(projects.values());
});

/**
 * Creating a project is an append, so its outcome must be classified like any
 * other. A producer duplicate is reconciled against the durable project row;
 * the request payload is never echoed back as if it had been accepted.
 */
export type CreateProjectResult =
  | { readonly status: "created" | "reconciled"; readonly project: Project }
  | { readonly status: "conflict" | "rejected"; readonly detail: string };

export const createProject = Effect.fn("Application.createProject")(function* (
  workspaceId: string,
  request: CreateProjectRequest,
) {
  yield* identifier("workspaceId", workspaceId);
  const streams = yield* Streams;
  const producers = yield* CommandProducers;

  yield* ensureAll([
    streamNames.projects(workspaceId),
    streamNames.membership(workspaceId, request.projectId),
    streamNames.board(workspaceId, request.projectId),
  ]);

  const project: Project = {
    projectId: request.projectId,
    projectKey: request.projectKey.toUpperCase(),
    name: request.name,
  };
  // Project rows are application-owned State, appended idempotently by key.
  const producer = yield* producers.forCommand(`project:${workspaceId}:${request.projectId}`);
  const appends = yield* AppendStreams;
  const outcome = yield* appends.appendJsonBatch(
    streams.bindings.projects(workspaceId),
    [
      {
        type: PROJECT_COLLECTION,
        key: project.projectId,
        value: project,
        headers: { operation: "upsert" },
      },
    ],
    { producer },
  );

  if (outcome.status === "appended") return { status: "created" as const, project };
  if (outcome.status === "duplicate") {
    // The producer sequence was already accepted. Payload equality is not
    // verified, so the durable row is the answer — not this request's body.
    const durable = (yield* listProjects(workspaceId)).find(
      (candidate) => candidate.projectId === request.projectId,
    );
    return durable === undefined
      ? {
          status: "conflict" as const,
          detail: "the producer sequence was accepted but no durable project row exists",
        }
      : { status: "reconciled" as const, project: durable };
  }
  return { status: "rejected" as const, detail: outcome.status };
});

/**
 * Demo-only issue key allocator: the number of durable join facts in the
 * project membership stream. Concurrent creation can repeat a display key; key
 * uniqueness is not a correctness law in this demo.
 */
const nextIssueKey = Effect.fn("Application.nextIssueKey")(function* (
  workspaceId: string,
  projectId: string,
  projectKey: string,
) {
  const streams = yield* Streams;
  const read = yield* readAllScoped(streams.bindings.membership(workspaceId, projectId));
  const joins =
    read.status === "ok"
      ? read.items.filter((item) => isRecord(item) && item.type === "IssueJoined").length
      : 0;
  return `${projectKey}-${100 + joins}`;
});

/**
 * Per-request command options.
 *
 * `deferProjections` skips the immediate projection passes so convergence has
 * to come from repair or the wake consumer. It changes no durability: the
 * command is appended and acknowledged exactly as usual. It exists so a smoke
 * can exercise the queue/repair path that a lost immediate pass depends on.
 */
export interface CommandOptions {
  readonly deferProjections?: boolean;
}

export const createIssue = Effect.fn("Application.createIssue")(function* (
  workspaceId: string,
  request: CreateIssueRequest,
  command: CommandOptions = {},
) {
  yield* identifier("workspaceId", workspaceId);
  const streams = yield* Streams;
  const producers = yield* CommandProducers;

  const projects = yield* listProjects(workspaceId);
  const project = projects.find((candidate) => candidate.projectId === request.projectId);
  if (project === undefined) {
    return yield* new UnknownProject({ projectId: request.projectId });
  }

  yield* ensureAll([
    streamNames.issueEvents(workspaceId, request.issueId),
    streamNames.issueDetail(workspaceId, request.issueId),
    streamNames.membership(workspaceId, request.projectId),
    streamNames.board(workspaceId, request.projectId),
  ]);

  const issueKey = yield* nextIssueKey(workspaceId, request.projectId, project.projectKey);
  const at = yield* now;
  const event: IssueEvent = {
    type: "IssueCreated",
    commandId: request.commandId,
    at,
    issueId: request.issueId,
    issueKey,
    projectId: request.projectId,
    title: request.title,
    status: request.status ?? "backlog",
    priority: request.priority ?? "medium",
    creatorId: request.creatorId ?? TEAM[0].id,
  };

  const producer = yield* producers.forCommand(request.commandId);
  const appended = yield* appendIssueEvent(
    streams.bindings.issueEvents(workspaceId, request.issueId),
    event,
    producer,
  );

  // Step two of the creation workflow. Its deterministic producer id makes a
  // repeated repair of a partly complete creation safe.
  const joinProducer = yield* producers.forCommand(
    membershipCommandId(request.commandId, request.issueId),
  );
  yield* appendMembershipFact(
    streams.bindings.membership(workspaceId, request.projectId),
    { type: "IssueJoined", issueId: request.issueId, from: null },
    joinProducer,
  );

  return yield* settle({
    workspaceId,
    issueId: request.issueId,
    projectId: request.projectId,
    commandId: request.commandId,
    ack: appended.ack,
    reconciled: appended.status === "reconciled",
    deferProjections: command.deferProjections === true,
  });
});

export const issueCommand = Effect.fn("Application.issueCommand")(function* (
  workspaceId: string,
  issueId: string,
  request: IssueCommandRequest,
  command: CommandOptions = {},
) {
  yield* identifier("workspaceId", workspaceId);
  yield* identifier("issueId", issueId);
  const streams = yield* Streams;
  const producers = yield* CommandProducers;

  const detail = yield* loadDetail(workspaceId, issueId);
  if (detail === undefined) return yield* new UnknownIssue({ issueId });

  const at = yield* now;
  const event = buildEvent(request, at);
  const producer = yield* producers.forCommand(request.commandId);
  const appended = yield* appendIssueEvent(
    streams.bindings.issueEvents(workspaceId, issueId),
    event,
    producer,
  );

  return yield* settle({
    workspaceId,
    issueId,
    projectId: detail.projectId,
    commandId: request.commandId,
    ack: appended.ack,
    reconciled: appended.status === "reconciled",
    deferProjections: command.deferProjections === true,
  });
});

/**
 * Run the affected projections immediately for low latency, then prove the
 * chain. Immediate work is an optimisation: a lost pass converges through the
 * wake consumer or the explicit repair endpoint.
 *
 * Every pass result is classified into the response. A caller may never read a
 * successful HTTP status as evidence that the board covers this command; only
 * `coverage.status === "proven"` with no faulted pass carries that meaning.
 */
const settle = Effect.fn("Application.settle")(function* (input: {
  readonly workspaceId: string;
  readonly issueId: string;
  readonly projectId: string;
  readonly commandId: string;
  readonly ack: SourceAck;
  readonly reconciled: boolean;
  /** Skip the immediate passes and let repair or the wake consumer converge. */
  readonly deferProjections: boolean;
}) {
  const { workspaceId } = input;
  const projections = input.deferProjections
    ? ([
        deferredPass("issue-detail"),
        deferredPass("project-board"),
      ] as readonly ProjectionPassReport[])
    : [
        yield* runPass("issue-detail", runIssueDetail(workspaceId, input.issueId)),
        yield* runPass("project-board", runProjectBoard(workspaceId, input.projectId)),
      ];

  const probe = yield* proveChain({
    workspaceId,
    issueId: input.issueId,
    projectId: input.projectId,
    ack: input.ack,
  });
  const detail = yield* loadDetail(workspaceId, input.issueId);

  const settled =
    probe.coverage.status === "proven" && projections.every((pass) => pass.outcome === "caught-up");
  if (!settled) {
    const wake = yield* Wake;
    yield* wake.wake({
      workspaceId,
      projectId: input.projectId,
      issueId: input.issueId,
    });
  }

  const response: MutationResponse = {
    commandId: input.commandId,
    issueId: input.issueId,
    projectId: input.projectId,
    ack: { stream: input.ack.identity.name, position: input.ack.position },
    reconciled: input.reconciled,
    coverage: coverageReport(workspaceId, input, probe),
    projections,
    detail: detail ?? null,
  };
  return response;
});

function deferredPass(label: ProjectionPassReport["label"]): ProjectionPassReport {
  return {
    label,
    status: "deferred",
    outcome: "deferred",
    detail: "the immediate pass was skipped by request",
  };
}

/**
 * Run one bounded pass and keep its verdict. A typed mesh error — restore
 * poison included — becomes a faulted pass rather than a lost status.
 * Interruption stays interruption.
 */
const runPass = <
  A extends Parameters<typeof classifyPass>[1],
  E extends { readonly _tag: string },
  R,
>(
  label: ProjectionPassReport["label"],
  pass: Effect.Effect<A, E, R>,
): Effect.Effect<ProjectionPassReport, never, R> =>
  pass.pipe(
    Effect.map((result) => classifyPass(label, result)),
    Effect.catch((error) => Effect.succeed(faultedPass(label, error))),
  );

/**
 * Read-only lineage probe for one accepted acknowledgement. It runs no
 * projection work, so it can only ever report what durable lineage already
 * shows.
 */
export const probeCoverage = Effect.fn("Application.probeCoverage")(function* (
  workspaceId: string,
  issueId: string,
  position: string,
) {
  yield* identifier("workspaceId", workspaceId);
  yield* identifier("issueId", issueId);
  if (position.length === 0) {
    return yield* InvalidRequest.of("position", "is required");
  }
  const streams = yield* Streams;
  const detail = yield* loadDetail(workspaceId, issueId);
  if (detail === undefined) return yield* new UnknownIssue({ issueId });

  const ack = sourceAck(streams.bindings.issueEvents(workspaceId, issueId).identity, position);
  const probe = yield* proveChain({ workspaceId, issueId, projectId: detail.projectId, ack });
  const response: CoverageResponse = {
    issueId,
    projectId: detail.projectId,
    coverage: coverageReport(workspaceId, { issueId, projectId: detail.projectId, ack }, probe),
    projections: [],
  };
  return response;
});

function coverageReport(
  workspaceId: string,
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
        source: streamNames.issueEvents(workspaceId, input.issueId),
        through: probe.detail.through,
        output: probe.detail.output,
      },
      {
        label: "project-board",
        source: streamNames.issueDetail(workspaceId, input.issueId),
        through: probe.board.through,
        output: probe.board.output,
      },
    ],
  };
}

export const loadDetail = Effect.fn("Application.loadDetail")(function* (
  workspaceId: string,
  issueId: string,
) {
  const streams = yield* Streams;
  const read = yield* readAllScoped(streams.bindings.issueDetail(workspaceId, issueId));
  if (read.status !== "ok") return undefined;
  let detail: IssueDetail | undefined;
  for (const item of read.items) {
    if (!isRecord(item) || item.type !== "issue-detail") continue;
    if (isRecord(item.headers) && item.headers.operation === "delete") {
      detail = undefined;
      continue;
    }
    detail = decodeIssueDetail(item.value);
  }
  return detail;
});

/** Load a known issue, or fail with the typed `UnknownIssue`. */
export const requireDetail = Effect.fn("Application.requireDetail")(function* (
  workspaceId: string,
  issueId: string,
) {
  yield* identifier("workspaceId", workspaceId);
  yield* identifier("issueId", issueId);
  const detail = yield* loadDetail(workspaceId, issueId);
  if (detail === undefined) return yield* new UnknownIssue({ issueId });
  return detail;
});

/** Bounded repair: catch up every active member and then the board itself. */
export const repairProject = Effect.fn("Application.repairProject")(function* (
  workspaceId: string,
  projectId: string,
) {
  yield* identifier("workspaceId", workspaceId);
  yield* identifier("projectId", projectId);
  const streams = yield* Streams;
  const membership = yield* readAllScoped(streams.bindings.membership(workspaceId, projectId));
  const active = new Set<string>();
  if (membership.status === "ok") {
    for (const item of membership.items) {
      if (!isRecord(item) || typeof item.issueId !== "string") continue;
      if (item.type === "IssueJoined") active.add(item.issueId);
      if (item.type === "IssueLeft") active.delete(item.issueId);
    }
  }
  const members = Array.from(active).toSorted();
  const passes: ProjectionPassReport[] = [];
  for (const issueId of members) {
    passes.push(yield* runPass("issue-detail", runIssueDetail(workspaceId, issueId)));
  }
  const board = yield* runPass("project-board", runProjectBoard(workspaceId, projectId));
  const response: RepairResponse = {
    repaired: members,
    board: board.status,
    projections: [...passes, board],
  };
  return response;
});

/**
 * Run both projections for one issue and its project. This is what the explicit
 * `sync` endpoint does; it names the same bounded work repair performs.
 */
export const syncIssue = Effect.fn("Application.syncIssue")(function* (
  workspaceId: string,
  issueId: string,
) {
  const detail = yield* requireDetail(workspaceId, issueId);
  yield* runIssueDetail(workspaceId, issueId);
  const board = yield* runProjectBoard(workspaceId, detail.projectId);
  return { issueId, projectId: detail.projectId, board: board.status };
});

/** Wall-clock reads go through the Effect clock, so a test can control them. */
const now = Effect.map(
  Effect.clockWith((clock) => clock.currentTimeMillis),
  (millis) => new Date(millis).toISOString(),
);

/**
 * Build the durable event for one already-decoded command.
 *
 * The request was decoded at the HTTP boundary, so every branch here is total:
 * there is nothing left to validate and nothing to throw.
 */
function buildEvent(request: IssueCommandRequest, at: string): IssueEvent {
  const base = { commandId: request.commandId, at };
  if (request.type === "rename") return { ...base, type: "IssueRenamed", title: request.title };
  if (request.type === "status") {
    return { ...base, type: "IssueStatusChanged", status: request.status };
  }
  if (request.type === "priority") {
    return { ...base, type: "IssuePriorityChanged", priority: request.priority };
  }
  if (request.type === "assign") {
    return { ...base, type: "IssueAssigned", assigneeId: request.assigneeId };
  }
  return {
    ...base,
    type: "CommentAdded",
    commentId: request.commentId,
    authorId: request.authorId,
    body: request.body,
  };
}

export const getBoard = Effect.fn("Application.getBoard")(function* (
  workspaceId: string,
  projectId: string,
) {
  yield* identifier("workspaceId", workspaceId);
  yield* identifier("projectId", projectId);
  const board = yield* readBoard(workspaceId, projectId);
  if (board.status !== "ready") return { status: board.status } as const;
  const response: BoardResponse = {
    projectId,
    boardStream: board.boardStream,
    rows: board.rows.toSorted((left, right) => left.issueKey.localeCompare(right.issueKey)),
  };
  return { status: "ok" as const, response };
});

export const health = Effect.fn("Application.health")(function* () {
  const config = yield* AppConfig;
  return {
    status: "ok",
    deployment: config.deployment,
    schemaVersion: config.schemaVersion,
    host: config.host,
  } satisfies HealthResponse;
});

export function projectsResponse(
  workspaceId: string,
  projects: readonly Project[],
): ProjectsResponse {
  return { workspaceId, projects };
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
