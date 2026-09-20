import { Schema } from "effect";

export const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
export const Identifier = Schema.String.check(Schema.isPattern(IDENTIFIER_PATTERN));
export const Title = Schema.String.check(Schema.isPattern(/^\s*\S[\s\S]{0,199}$/));
export const Timestamp = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/),
);
export const Sequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export const IssueStatus = Schema.Literals(["backlog", "todo", "in_progress", "done"]);
export type IssueStatus = typeof IssueStatus.Type;

const common = {
  eventId: Identifier,
  workspaceId: Identifier,
  issueId: Identifier,
  sequence: Sequence,
  occurredAt: Timestamp,
} as const;
export const IssueCreated = Schema.Struct({
  type: Schema.Literal("IssueCreated"), ...common, title: Title, projectId: Identifier, status: IssueStatus,
});
export const IssueStatusChanged = Schema.Struct({
  type: Schema.Literal("IssueStatusChanged"), ...common, status: IssueStatus,
});
export const IssueAssigned = Schema.Struct({
  type: Schema.Literal("IssueAssigned"), ...common, status: IssueStatus, assigneeId: Identifier,
});
export const IssueEvent = Schema.Union([IssueCreated, IssueStatusChanged, IssueAssigned]);
export type IssueEvent = typeof IssueEvent.Type;

export const IssueRow = Schema.Struct({
  issueId: Identifier, workspaceId: Identifier, projectId: Identifier, title: Title,
  status: IssueStatus, sequence: Sequence, updatedAt: Timestamp,
  assigneeId: Schema.optionalKey(Identifier),
});
export type IssueRow = typeof IssueRow.Type;

export const MembershipId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$/));
export const membershipIdOf = (issueId: string, labelId: string): string => `${issueId}.${labelId}`;
const membership = {
  eventId: Identifier, workspaceId: Identifier, issueId: Identifier, labelId: Identifier,
  membershipId: MembershipId, sequence: Sequence, occurredAt: Timestamp,
} as const;
export const LabelAttached = Schema.Struct({ type: Schema.Literal("LabelAttached"), ...membership });
export const LabelDetached = Schema.Struct({ type: Schema.Literal("LabelDetached"), ...membership });
export const IssueLabelEvent = Schema.Union([LabelAttached, LabelDetached]);
export type IssueLabelEvent = typeof IssueLabelEvent.Type;
export const IssueLabelRow = Schema.Struct({
  membershipId: MembershipId, issueId: Identifier, labelId: Identifier,
  workspaceId: Identifier, attached: Schema.Boolean, sequence: Sequence, updatedAt: Timestamp,
});
export type IssueLabelRow = typeof IssueLabelRow.Type;

/** Arrival-order fold: an older sequence can never replace a newer issue row. */
export function foldIssue(previous: IssueRow | undefined, event: IssueEvent): IssueRow | undefined {
  if (previous !== undefined && event.sequence <= previous.sequence) return previous;
  if (event.type === "IssueCreated") return {
    issueId: event.issueId, workspaceId: event.workspaceId, projectId: event.projectId,
    title: event.title, status: event.status, sequence: event.sequence, updatedAt: event.occurredAt,
  };
  if (previous === undefined) return undefined;
  return event.type === "IssueAssigned"
    ? { ...previous, assigneeId: event.assigneeId, status: event.status, sequence: event.sequence, updatedAt: event.occurredAt }
    : { ...previous, status: event.status, sequence: event.sequence, updatedAt: event.occurredAt };
}
