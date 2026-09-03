/**
 * Projection orchestration for the fixed demo path:
 *
 *   IssueEvents(issueId) -> IssueDetail(issueId) -> ProjectBoard(projectId)
 *
 * `IssueDetail` is a recovered single-source State projection. `ProjectBoard` is
 * a deterministic dynamic fan-in over the project's active issue details. Both
 * are bounded; a wake only affects latency, and durable lineage remains the
 * authority.
 *
 * Every operation here is an Effect description that reads its bindings from
 * `Streams` and its producer lanes from `ProjectionLanes`. No context object is
 * threaded through call sites, and nothing converts a Promise inline.
 */
import type { JsonValue } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/streams/binding";
import { sourceAck, sourceWatermark, type SourceAck } from "@streamsy/streams/causal";
import type { StreamIdentity } from "@streamsy/streams/identity";
import { StateRestorePoison } from "@streamsy/streams";
import {
  catchUpDynamicFanInState,
  catchUpState,
  chainedCoverage,
  FanInRecovery,
  recoverDerivedState,
  type CatchUpFanInResult,
  type CatchUpStateResult,
  type ChainHop,
  type ChainedCoverage,
  type MembershipChange,
} from "@streamsy/projection/mesh";
import { Effect, Schema } from "effect";
import type { ProjectionPassReport } from "../shared/api.ts";
import {
  BOARD_ROW_COLLECTION,
  boardRow,
  decodeBoardRow,
  decodeIssueDetail,
  decodeStateFact,
  evolveIssue,
  ISSUE_DETAIL_COLLECTION,
  IssueEvent,
  ProjectMembershipFact,
  isStateFact,
  streamNames,
  type BoardRow,
  type IssueDetail,
} from "../shared/domain.ts";
import { ProjectionLanes } from "./lanes.ts";
import { PROJECTION_LIMITS, Streams } from "./streams.ts";

export type DetailState = IssueDetail | undefined;
export type BoardState = Readonly<Record<string, BoardRow>>;

const decodeIssueEvent = Schema.decodeUnknownSync(IssueEvent);
const decodeMembershipFact = Schema.decodeUnknownSync(ProjectMembershipFact);

function detailFact(detail: IssueDetail): JsonValue {
  return {
    type: ISSUE_DETAIL_COLLECTION,
    key: detail.issueId,
    value: detail,
    headers: { operation: "upsert" },
  };
}

function boardFact(row: BoardRow): JsonValue {
  return {
    type: BOARD_ROW_COLLECTION,
    key: row.issueId,
    value: row,
    headers: { operation: "upsert" },
  };
}

function boardRemoval(issueId: string): JsonValue {
  return { type: BOARD_ROW_COLLECTION, key: issueId, headers: { operation: "delete" } };
}

/**
 * Restore issue detail from durable target State alone.
 *
 * Both the collection tag and the application value are checked. A correctly
 * tagged row carrying a malformed value throws here, and the kernel turns that
 * throw into a typed `StateRestorePoison` rather than accepting the value.
 */
export function restoreDetail(initial: DetailState, facts: readonly JsonValue[]): DetailState {
  let state = initial;
  for (const encoded of facts) {
    const fact = decodeStateFact(encoded);
    if (fact.type !== ISSUE_DETAIL_COLLECTION) {
      throw new TypeError("Unexpected fact in the issue-detail State stream");
    }
    if (fact.headers?.operation === "delete") {
      state = undefined;
      continue;
    }
    state = decodeIssueDetail(fact.value);
  }
  return state;
}

export function restoreBoard(initial: BoardState, facts: readonly JsonValue[]): BoardState {
  let board = initial;
  for (const encoded of facts) {
    const fact = decodeStateFact(encoded);
    if (fact.type !== BOARD_ROW_COLLECTION || fact.key === undefined) {
      throw new TypeError("Unexpected fact in the project-board State stream");
    }
    if (fact.headers?.operation === "delete") {
      const { [fact.key]: _removed, ...rest } = board;
      board = rest;
      continue;
    }
    board = { ...board, [fact.key]: decodeBoardRow(fact.value) };
  }
  return board;
}

/** Run one bounded `IssueEvents -> IssueDetail` pass. */
export const runIssueDetail = Effect.fn("Projections.issueDetail")(function* (
  workspaceId: string,
  issueId: string,
) {
  const streams = yield* Streams;
  const lanes = yield* ProjectionLanes;
  const lane = yield* lanes.issueDetail(workspaceId, issueId);
  return yield* catchUpState<DetailState, IssueEvent>({
    source: streams.bindings.issueEvents(workspaceId, issueId),
    target: streams.bindings.issueDetail(workspaceId, issueId),
    lane,
    limits: PROJECTION_LIMITS,
    initial: undefined,
    restore: restoreDetail,
    validateRecovered: () => {},
    decode(batch) {
      if (batch.kind !== "json") throw new TypeError("Issue events must be JSON");
      return batch.items.map((item) => decodeIssueEvent(item));
    },
    step(state, events) {
      let next = state;
      for (const event of events) next = evolveIssue(next, event);
      if (next === undefined) return { facts: [] };
      return { facts: [detailFact(next)] };
    },
  });
});

/** Run one bounded `ProjectMembership + IssueDetail* -> ProjectBoard` pass. */
export const runProjectBoard = Effect.fn("Projections.projectBoard")(function* (
  workspaceId: string,
  projectId: string,
) {
  const streams = yield* Streams;
  const lanes = yield* ProjectionLanes;
  const lane = yield* lanes.projectBoard(workspaceId, projectId);
  return yield* catchUpDynamicFanInState<BoardState, IssueDetail>({
    membership: streams.bindings.membership(workspaceId, projectId),
    target: streams.bindings.board(workspaceId, projectId),
    lane,
    limits: PROJECTION_LIMITS,
    initial: {},
    restore: restoreBoard,
    decodeMembership(batch) {
      if (batch.kind !== "json") throw new TypeError("Membership facts must be JSON");
      return batch.items.map((item): MembershipChange => {
        const fact = decodeMembershipFact(item);
        const member = streams.bindings.issueDetail(workspaceId, fact.issueId).identity;
        if (fact.type === "IssueLeft") return { type: "leave", member };
        if (fact.from === null) return { type: "join", member };
        return { type: "join", member, from: fact.from };
      });
    },
    resolveMember: (identity) => resolveDetailMember(streams, workspaceId, identity),
    decodeMember(batch) {
      if (batch.kind !== "json") throw new TypeError("Issue detail State must be JSON");
      return batch.items.flatMap((item) =>
        isStateFact(item) && item.type === ISSUE_DETAIL_COLLECTION && item.value !== undefined
          ? [decodeIssueDetail(item.value)]
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
      const issueId = issueIdFromDetailIdentity(workspaceId, member.identity);
      if (issueId === undefined) return { state, facts: [] };
      const { [issueId]: _removed, ...rest } = state;
      return { state: rest, facts: [boardRemoval(issueId)] };
    },
  });
});

function detailPrefix(workspaceId: string): string {
  return `workspaces/${workspaceId}/issues/`;
}

function issueIdFromDetailIdentity(
  workspaceId: string,
  identity: StreamIdentity,
): string | undefined {
  const prefix = detailPrefix(workspaceId);
  if (!identity.name.startsWith(prefix) || !identity.name.endsWith("/detail")) return undefined;
  const issueId = identity.name.slice(prefix.length, -"/detail".length);
  return issueId.includes("/") || issueId.length === 0 ? undefined : issueId;
}

/**
 * Only issue-detail streams inside this workspace are resolvable members.
 * Anything else becomes an explicit `unknown-member` status.
 */
function resolveDetailMember(
  streams: Streams["Service"],
  workspaceId: string,
  identity: StreamIdentity,
): StreamBinding | undefined {
  const issueId = issueIdFromDetailIdentity(workspaceId, identity);
  return issueId === undefined ? undefined : streams.bindings.issueDetail(workspaceId, issueId);
}

/** A projection pass result, as the two kernels report it. */
type PassResult = CatchUpStateResult<DetailState> | CatchUpFanInResult<BoardState>;

/**
 * Classify one bounded pass.
 *
 * Only `caught-up` may ever support a `Synced` claim. A bounded stop that
 * repair can resume is `deferred`; anything that cannot progress on its own —
 * an output conflict, a producer fault, an unknown member, an oversized
 * boundary, or a target that is gone — is `faulted` and stays visible.
 */
export function classifyPass(
  label: ProjectionPassReport["label"],
  result: PassResult,
): ProjectionPassReport {
  const report = (
    outcome: ProjectionPassReport["outcome"],
    detail?: string,
  ): ProjectionPassReport => {
    if (detail === undefined) return { label, status: result.status, outcome };
    return { label, status: result.status, outcome, detail };
  };
  switch (result.status) {
    case "caught-up":
      return report("caught-up");
    case "limit-reached":
      return report("deferred", `bounded stop at ${result.limit}`);
    case "missing":
      return report("deferred", `${result.stream} stream does not exist yet`);
    case "boundary-too-large":
      return report("faulted", `${result.limit} ${result.actual} exceeds ${result.maximum}`);
    case "gone":
      return report("faulted", `${result.stream} stream is gone`);
    case "unknown-member":
      return report("faulted", `unresolvable member ${result.member}`);
    case "output-conflict":
      return report("faulted", result.reason);
    default:
      return report("faulted", "durable producer lineage rejected the commit");
  }
}

const MESH_ERROR_DETAIL = new Map([
  ["StateRestorePoison", "durable target State could not be restored into typed application state"],
  ["ProjectionPoison", "the projection could not process a boundary"],
]);

/** A typed mesh error — poison included — is a fault, never a silent success. */
export function faultedPass(
  label: ProjectionPassReport["label"],
  error: { readonly _tag: string },
): ProjectionPassReport {
  const { _tag: tag } = error;
  return {
    label,
    status: tag,
    outcome: "faulted",
    detail: MESH_ERROR_DETAIL.get(tag) ?? "the projection pass failed",
  };
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
export const proveChain = Effect.fn("Projections.proveChain")(function* (options: {
  readonly workspaceId: string;
  readonly issueId: string;
  readonly projectId: string;
  readonly ack: SourceAck;
}) {
  const { workspaceId } = options;
  const streams = yield* Streams;
  const lanes = yield* ProjectionLanes;
  const detailLane = yield* lanes.issueDetail(workspaceId, options.issueId);
  const boardLane = yield* lanes.projectBoard(workspaceId, options.projectId);
  const detailIdentity = streams.bindings.issueDetail(workspaceId, options.issueId).identity;

  const detailRecovered = yield* recoverDerivedState(
    streams.bindings.issueDetail(workspaceId, options.issueId),
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
    streams.bindings.board(workspaceId, options.projectId),
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

  const eventsIdentity = streams.bindings.issueEvents(workspaceId, options.issueId).identity;
  const boardIdentity = streams.bindings.board(workspaceId, options.projectId).identity;

  const detailHop: ChainHop =
    detailThrough === null || detailOutput === null
      ? { label: "issue-detail" }
      : {
          label: "issue-detail",
          watermark: sourceWatermark(eventsIdentity, detailThrough),
          output: sourceAck(detailIdentity, detailOutput),
        };
  const boardHop: ChainHop =
    boardThrough === null || boardOutput === null
      ? { label: "project-board" }
      : {
          label: "project-board",
          watermark: sourceWatermark(detailIdentity, boardThrough),
          output: sourceAck(boardIdentity, boardOutput),
        };
  const coverage = chainedCoverage(options.ack, [detailHop, boardHop]);

  return {
    coverage,
    detail: { through: detailThrough, output: detailOutput },
    board: { through: boardThrough, output: boardOutput },
  } satisfies ChainProbe;
});

export const readBoard = Effect.fn("Projections.readBoard")(function* (
  workspaceId: string,
  projectId: string,
) {
  const streams = yield* Streams;
  const lanes = yield* ProjectionLanes;
  const lane = yield* lanes.projectBoard(workspaceId, projectId);
  const fanIn = yield* FanInRecovery;
  const recovered = yield* fanIn.recoverFanIn(streams.bindings.board(workspaceId, projectId), lane);
  if (recovered.status !== "ready") return { status: recovered.status } as const;
  // Serving durable rows uses the same schema-backed restore the kernel uses,
  // so a malformed row is typed poison here too instead of a served lie.
  const board = yield* Effect.try({
    try: () => restoreBoard({}, recovered.facts),
    catch: (cause) =>
      new StateRestorePoison({ targetOffset: recovered.checkpoint.targetOffset, cause }),
  });
  return {
    status: "ready" as const,
    rows: Object.values(board),
    boardStream: streamNames.board(workspaceId, projectId),
  };
});
