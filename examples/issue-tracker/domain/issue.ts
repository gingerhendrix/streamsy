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
});
export type IssueRow = typeof IssueRow.Type;

export const decodeIssueEvent = Schema.decodeUnknownSync(IssueEvent);
export const encodeIssueEventJson = Schema.encodeUnknownSync(Schema.fromJsonString(IssueEvent));
export const decodeIssueRow = Schema.decodeUnknownSync(IssueRow);

/** Board column order, left to right. */
export const BOARD_COLUMNS: readonly { readonly status: IssueStatus; readonly label: string }[] = [
  { status: "backlog", label: "Backlog" },
  { status: "todo", label: "Todo" },
  { status: "in_progress", label: "In progress" },
  { status: "done", label: "Done" },
];
