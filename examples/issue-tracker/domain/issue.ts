/**
 * The slice's domain: two canonical issue events and one maintained row.
 *
 * `eventId` is source identity, `sequence` is the deterministic order inside one
 * workspace stream, and `issueId` is the maintained row key. `commandId` lives
 * at the HTTP edge instead of on the event, because it identifies a *request*
 * and the durable fact must stay meaningful after the request is forgotten.
 *
 * Timestamps are ISO-8601 strings rather than the drafted `Schema.DateTimeUtc`.
 * The same value crosses the JSON event stream, a SQLite column, the Durable
 * State wire, and a TanStack DB row; a string keeps all four byte-identical and
 * removes an encode/decode asymmetry the slice would otherwise have to test.
 */
import { Schema } from "effect";

export const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
export const Identifier = Schema.String.check(Schema.isPattern(IDENTIFIER_PATTERN));

export const Title = Schema.String.check(
  Schema.isPattern(/^\s*\S[\s\S]{0,199}$/, {
    title: "a non-blank title of at most 200 characters",
  }),
);

/** An ISO-8601 UTC instant, checked at the boundary and stored verbatim. */
export const TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;
export const Timestamp = Schema.String.check(Schema.isPattern(TIMESTAMP_PATTERN));

/** A source order value: a non-negative integer. */
export const Sequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

export const IssueStatus = Schema.Literals(["backlog", "todo", "in_progress", "done"]);
export type IssueStatus = typeof IssueStatus.Type;

export const ISSUE_STATUSES = ["backlog", "todo", "in_progress", "done"] as const;

export const IssueCreated = Schema.Struct({
  type: Schema.Literal("IssueCreated"),
  eventId: Identifier,
  workspaceId: Identifier,
  issueId: Identifier,
  sequence: Sequence,
  occurredAt: Timestamp,
  title: Title,
  projectId: Identifier,
  status: IssueStatus,
});

export const IssueStatusChanged = Schema.Struct({
  type: Schema.Literal("IssueStatusChanged"),
  eventId: Identifier,
  workspaceId: Identifier,
  issueId: Identifier,
  sequence: Sequence,
  occurredAt: Timestamp,
  status: IssueStatus,
});

export const IssueEvent = Schema.Union([IssueCreated, IssueStatusChanged]);
export type IssueEvent = typeof IssueEvent.Type;

export const IssueRow = Schema.Struct({
  issueId: Identifier,
  workspaceId: Identifier,
  projectId: Identifier,
  title: Title,
  status: IssueStatus,
  updatedAt: Timestamp,
  assigneeId: Schema.optionalKey(Identifier),
});
export type IssueRow = typeof IssueRow.Type;

export const decodeIssueEvent = Schema.decodeUnknownSync(IssueEvent);
export const encodeIssueEventJson = Schema.encodeUnknownSync(Schema.fromJsonString(IssueEvent));
export const decodeIssueRow = Schema.decodeUnknownSync(IssueRow);
export const decodeIdentifier = Schema.decodeUnknownSync(Identifier);

/** Supporting A1 relation rows. Their ingestion and runtime maintenance belong to later tracks. */
export const ProjectRow = Schema.Struct({
  projectId: Identifier,
  workspaceId: Identifier,
  name: Title,
  revision: Sequence,
});
export type ProjectRow = typeof ProjectRow.Type;

export const UserRow = Schema.Struct({
  userId: Identifier,
  workspaceId: Identifier,
  displayName: Title,
  revision: Sequence,
});
export type UserRow = typeof UserRow.Type;

export const LabelRow = Schema.Struct({
  labelId: Identifier,
  workspaceId: Identifier,
  name: Title,
  revision: Sequence,
});
export type LabelRow = typeof LabelRow.Type;

export const IssueLabelRow = Schema.Struct({
  issueId: Identifier,
  labelId: Identifier,
  revision: Sequence,
});
export type IssueLabelRow = typeof IssueLabelRow.Type;

export const ProjectBoardCard = Schema.Struct({
  issueId: Identifier,
  projectId: Identifier,
  projectName: Title,
  title: Title,
  status: IssueStatus,
  assignee: Schema.String,
  updatedAt: Timestamp,
});
export type ProjectBoardCard = typeof ProjectBoardCard.Type;
export const decodeProjectBoardCard = Schema.decodeUnknownSync(ProjectBoardCard);

/**
 * One published issue transition.
 *
 * A transition is a change to the maintained `issue-tracker.issues` relation,
 * not a restatement of a canonical fact: `change` is the shape of that change
 * and `occurredAt` is the `updatedAt` the fold produced. Because a fold observes
 * Durable Stream arrival order, a feed of these is in arrival order too.
 */
export const IssueTransition = Schema.Struct({
  workspaceId: Identifier,
  issueId: Identifier,
  change: Schema.Literals(["enter", "update", "exit"]),
  status: IssueStatus,
  /** The status the row held before this change. Absent on entry and exit. */
  previousStatus: Schema.optionalKey(IssueStatus),
  title: Title,
  occurredAt: Timestamp,
});
export type IssueTransition = typeof IssueTransition.Type;
export const decodeIssueTransition = Schema.decodeUnknownSync(IssueTransition);

/** Issue totals per declared board column. Every column is present, including empty ones. */
export const IssueStatusCounts = Schema.Struct({
  backlog: Sequence,
  todo: Sequence,
  in_progress: Sequence,
  done: Sequence,
});
export type IssueStatusCounts = typeof IssueStatusCounts.Type;

/**
 * The cached workspace summary document.
 *
 * It is derived from the maintained relation and the catalog, and it carries
 * the plan identity it was derived under, so a consumer holding a cached
 * summary can tell a stale plan from a stale count.
 */
export const WorkspaceSummary = Schema.Struct({
  workspaceId: Identifier,
  planHash: Schema.String.check(Schema.isPattern(/^[0-9a-f]{8}$/)),
  issues: Schema.Struct({ total: Sequence, byStatus: IssueStatusCounts }),
  catalog: Schema.Struct({ projects: Sequence, users: Sequence, labels: Sequence }),
  /** The latest `updatedAt` across the maintained rows, or null in an empty workspace. */
  latestActivityAt: Schema.NullOr(Timestamp),
});
export type WorkspaceSummary = typeof WorkspaceSummary.Type;
export const decodeWorkspaceSummary = Schema.decodeUnknownSync(WorkspaceSummary);

export const AssigneeQueueRow = Schema.Struct({
  issueId: Identifier,
  assigneeId: Identifier,
  assigneeName: Title,
  title: Title,
  status: IssueStatus,
  updatedAt: Timestamp,
});
export type AssigneeQueueRow = typeof AssigneeQueueRow.Type;

export const LabelCountRow = Schema.Struct({
  labelId: Identifier,
  labelName: Title,
  issueCount: Schema.Int,
});
export type LabelCountRow = typeof LabelCountRow.Type;

export const RecentActivityRow = Schema.Struct({
  eventId: Identifier,
  issueId: Identifier,
  eventType: Schema.Literals(["IssueCreated", "IssueStatusChanged"]),
  status: IssueStatus,
  sequence: Sequence,
  occurredAt: Timestamp,
});
export type RecentActivityRow = typeof RecentActivityRow.Type;

/** Board column order, left to right. */
export const BOARD_COLUMNS: readonly { readonly status: IssueStatus; readonly label: string }[] = [
  { status: "backlog", label: "Backlog" },
  { status: "todo", label: "Todo" },
  { status: "in_progress", label: "In progress" },
  { status: "done", label: "Done" },
];
