/**
 * Projection orchestration for the fixed demo path:
 *
 *   IssueEvents(issueId) -> IssueDetail(issueId) -> ProjectBoard(projectId)
 *
 * `IssueDetail` is a recovered single-source State projection. `ProjectBoard` is
 * a deterministic dynamic fan-in over the project's active issue details. Both
 * are bounded; a wake only affects latency, and durable lineage remains the
 * authority.
 */
import type { JsonValue, StreamProtocolClient } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import {
  sourceAck,
  sourceWatermark,
  type SourceAck,
  type StreamIdentity,
} from "@streamsy/experimental/causal";
import {
  catchUpDynamicFanInState,
  catchUpState,
  chainedCoverage,
  FanInRecovery,
  recoverDerivedState,
  type CatchUpFanInResult,
  type CatchUpStateResult,
  type ChainedCoverage,
  type MembershipChange,
} from "@streamsy/experimental/ivm-mesh";
import { Effect, Schema } from "effect";
import {
  BOARD_ROW_COLLECTION,
  boardRow,
  evolveIssue,
  ISSUE_DETAIL_COLLECTION,
  IssueEvent,
  ProjectMembershipFact,
  streamNames,
  type BoardRow,
  type IssueDetail,
} from "../shared/domain.ts";
import { LaneRegistry, PROJECTION_LIMITS, workspaceBindings } from "./bindings.ts";

export interface ProjectionContext {
  readonly client: StreamProtocolClient;
  readonly workspaceId: string;
  readonly bindings: ReturnType<typeof workspaceBindings>;
  readonly lanes: LaneRegistry;
}

export function projectionContext(
  client: StreamProtocolClient,
  workspaceId: string,
  lanes: LaneRegistry,
): ProjectionContext {
  return { client, workspaceId, bindings: workspaceBindings(client), lanes };
}

export type DetailState = IssueDetail | undefined;
export type BoardState = Readonly<Record<string, BoardRow>>;

const decodeIssueEvent = Schema.decodeUnknownSync(IssueEvent);
const decodeMembershipFact = Schema.decodeUnknownSync(ProjectMembershipFact);

function detailFact(detail: IssueDetail): JsonValue {
  return {
    type: ISSUE_DETAIL_COLLECTION,
    key: detail.issueId,
    value: detail as unknown as JsonValue,
    headers: { operation: "upsert" },
  };
}

function boardFact(row: BoardRow): JsonValue {
  return {
    type: BOARD_ROW_COLLECTION,
    key: row.issueId,
    value: row as unknown as JsonValue,
    headers: { operation: "upsert" },
  };
}

function boardRemoval(issueId: string): JsonValue {
  return { type: BOARD_ROW_COLLECTION, key: issueId, headers: { operation: "delete" } };
}

/** Restore issue detail from durable target State alone. */
function restoreDetail(initial: DetailState, facts: readonly JsonValue[]): DetailState {
  let state = initial;
  for (const fact of facts) {
    if (!isRecord(fact) || fact.type !== ISSUE_DETAIL_COLLECTION) {
      throw new TypeError("Unexpected fact in the issue-detail State stream");
    }
    if (isRecord(fact.headers) && fact.headers.operation === "delete") {
      state = undefined;
      continue;
    }
    state = fact.value as unknown as IssueDetail;
  }
  return state;
}

function restoreBoard(initial: BoardState, facts: readonly JsonValue[]): BoardState {
  let board = initial;
  for (const fact of facts) {
    if (!isRecord(fact) || fact.type !== BOARD_ROW_COLLECTION || typeof fact.key !== "string") {
      throw new TypeError("Unexpected fact in the project-board State stream");
    }
    if (isRecord(fact.headers) && fact.headers.operation === "delete") {
      const { [fact.key]: _removed, ...rest } = board;
      board = rest;
      continue;
    }
    board = { ...board, [fact.key]: fact.value as unknown as BoardRow };
  }
  return board;
}

/** Run one bounded `IssueEvents -> IssueDetail` pass. */
export const runIssueDetail = Effect.fn("Projections.issueDetail")(function* (
  context: ProjectionContext,
  issueId: string,
) {
  const lane = yield* Effect.promise(() => context.lanes.issueDetail(context.workspaceId, issueId));
  return (yield* catchUpState<DetailState, IssueEvent>({
    source: context.bindings.issueEvents(context.workspaceId, issueId),
    target: context.bindings.issueDetail(context.workspaceId, issueId),
    lane,
    limits: PROJECTION_LIMITS,
    initial: undefined,
    restore: restoreDetail,
    decode(batch) {
      if (batch.kind !== "json") throw new TypeError("Issue events must be JSON");
      return batch.items.map((item) => decodeIssueEvent(item));
    },
    step(state, events) {
      let next = state;
      for (const event of events) next = evolveIssue(next, event);
      if (next === undefined) return { state: next, facts: [] };
      return { state: next, facts: [detailFact(next)] };
    },
  })) as CatchUpStateResult<DetailState>;
});

/** Run one bounded `ProjectMembership + IssueDetail* -> ProjectBoard` pass. */
export const runProjectBoard = Effect.fn("Projections.projectBoard")(function* (
  context: ProjectionContext,
  projectId: string,
) {
  const lane = yield* Effect.promise(() =>
    context.lanes.projectBoard(context.workspaceId, projectId),
  );
  return (yield* catchUpDynamicFanInState<BoardState, IssueDetail>({
    membership: context.bindings.membership(context.workspaceId, projectId),
    target: context.bindings.board(context.workspaceId, projectId),
    lane,
    limits: PROJECTION_LIMITS,
    initial: {},
    restore: restoreBoard,
    decodeMembership(batch) {
      if (batch.kind !== "json") throw new TypeError("Membership facts must be JSON");
      return batch.items.map((item): MembershipChange => {
        const fact = decodeMembershipFact(item);
        const member = context.bindings.issueDetail(context.workspaceId, fact.issueId).identity;
        return fact.type === "IssueJoined"
          ? { type: "join", member, ...(fact.from === null ? {} : { from: fact.from }) }
          : { type: "leave", member };
      });
    },
    resolveMember: (identity) => resolveDetailMember(context, identity),
    decodeMember(batch) {
      if (batch.kind !== "json") throw new TypeError("Issue detail State must be JSON");
      return batch.items.flatMap((item) =>
        isRecord(item) && item.type === ISSUE_DETAIL_COLLECTION && isRecord(item.value)
          ? [item.value as unknown as IssueDetail]
          : [],
      );
    },
    onRecord(state, _member, details) {
      const latest = details.at(-1);
      if (latest === undefined) return { state, facts: [] };
      const row = boardRow(latest);
      return { state: { ...state, [row.issueId]: row }, facts: [boardFact(row)] };
    },
    onRemove(state, member) {
      const issueId = issueIdFromDetailIdentity(context, member.identity);
      if (issueId === undefined) return { state, facts: [] };
      const { [issueId]: _removed, ...rest } = state;
      return { state: rest, facts: [boardRemoval(issueId)] };
    },
  })) as CatchUpFanInResult<BoardState>;
});

function detailPrefix(context: ProjectionContext): string {
  return `workspaces/${context.workspaceId}/issues/`;
}

function issueIdFromDetailIdentity(
  context: ProjectionContext,
  identity: StreamIdentity,
): string | undefined {
  const prefix = detailPrefix(context);
  if (!identity.name.startsWith(prefix) || !identity.name.endsWith("/detail")) return undefined;
  const issueId = identity.name.slice(prefix.length, -"/detail".length);
  return issueId.includes("/") || issueId.length === 0 ? undefined : issueId;
}

/**
 * Only issue-detail streams inside this workspace are resolvable members.
 * Anything else becomes an explicit `unknown-member` status.
 */
function resolveDetailMember(
  context: ProjectionContext,
  identity: StreamIdentity,
): StreamBinding | undefined {
  const issueId = issueIdFromDetailIdentity(context, identity);
  return issueId === undefined
    ? undefined
    : context.bindings.issueDetail(context.workspaceId, issueId);
}

export interface ChainProbe {
  readonly coverage: ChainedCoverage;
  readonly detail: { readonly through: string | null; readonly output: string | null };
  readonly board: { readonly through: string | null; readonly output: string | null };
}

/**
 * Prove the fixed two-hop path by reading direct-source lineage at each output.
 * A wake receipt or elapsed delay can never produce `proven`.
 */
export const proveChain = Effect.fn("Projections.proveChain")(function* (
  context: ProjectionContext,
  options: { readonly issueId: string; readonly projectId: string; readonly ack: SourceAck },
) {
  const { workspaceId } = context;
  const detailLane = yield* Effect.promise(() =>
    context.lanes.issueDetail(workspaceId, options.issueId),
  );
  const boardLane = yield* Effect.promise(() =>
    context.lanes.projectBoard(workspaceId, options.projectId),
  );
  const detailIdentity = context.bindings.issueDetail(workspaceId, options.issueId).identity;

  const detailRecovered = yield* recoverDerivedState(
    context.bindings.issueDetail(workspaceId, options.issueId),
    detailLane,
  );
  const detailThrough =
    detailRecovered.status === "ready" ? (detailRecovered.sourceThrough ?? null) : null;
  const detailOutput =
    detailRecovered.status === "ready" && detailRecovered.sourceThrough !== undefined
      ? detailRecovered.targetOffset
      : null;

  const fanIn = yield* FanInRecovery;
  const boardRecovered = yield* fanIn.recoverFanIn(
    context.bindings.board(workspaceId, options.projectId),
    boardLane,
  );
  const memberRow =
    boardRecovered.status === "ready"
      ? boardRecovered.members.find((member) => member.identity.name === detailIdentity.name)
      : undefined;
  const boardThrough = memberRow?.through ?? null;
  const boardOutput =
    boardRecovered.status === "ready" && boardThrough !== null
      ? boardRecovered.checkpoint.targetOffset
      : null;

  const eventsIdentity = context.bindings.issueEvents(workspaceId, options.issueId).identity;
  const boardIdentity = context.bindings.board(workspaceId, options.projectId).identity;

  const coverage = chainedCoverage(options.ack, [
    {
      label: "issue-detail",
      ...(detailThrough === null || detailOutput === null
        ? {}
        : {
            watermark: sourceWatermark(eventsIdentity, detailThrough),
            output: sourceAck(detailIdentity, detailOutput),
          }),
    },
    {
      label: "project-board",
      ...(boardThrough === null || boardOutput === null
        ? {}
        : {
            watermark: sourceWatermark(detailIdentity, boardThrough),
            output: sourceAck(boardIdentity, boardOutput),
          }),
    },
  ]);

  return {
    coverage,
    detail: { through: detailThrough, output: detailOutput },
    board: { through: boardThrough, output: boardOutput },
  } satisfies ChainProbe;
});

export const readBoard = Effect.fn("Projections.readBoard")(function* (
  context: ProjectionContext,
  projectId: string,
) {
  const lane = yield* Effect.promise(() =>
    context.lanes.projectBoard(context.workspaceId, projectId),
  );
  const fanIn = yield* FanInRecovery;
  const recovered = yield* fanIn.recoverFanIn(
    context.bindings.board(context.workspaceId, projectId),
    lane,
  );
  if (recovered.status !== "ready") return { status: recovered.status } as const;
  return {
    status: "ready" as const,
    rows: Object.values(restoreBoard({}, recovered.facts)),
    boardStream: streamNames.board(context.workspaceId, projectId),
  };
});

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
