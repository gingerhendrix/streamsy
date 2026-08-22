/**
 * Web-standard domain values shared by the browser, the local host, and the
 * Worker.
 *
 * Effect Schema owns the shared browser/server contracts so both sides decode
 * the same durable and wire values.
 */
import { Schema } from "effect";

export const ISSUE_STATUSES = ["backlog", "in-progress", "done"] as const;
export const IssueStatusSchema = Schema.Literals(ISSUE_STATUSES);
export type IssueStatus = typeof IssueStatusSchema.Type;

/** True when an arbitrary string is one of the three board statuses. */
export const isIssueStatus = Schema.is(IssueStatusSchema);

export const ISSUE_PRIORITIES = ["low", "medium", "high", "urgent"] as const;
export const IssuePrioritySchema = Schema.Literals(ISSUE_PRIORITIES);
export type IssuePriority = typeof IssuePrioritySchema.Type;

/** True when an arbitrary string is one of the four issue priorities. */
export const isIssuePriority = Schema.is(IssuePrioritySchema);

export const TEAM = [
  { id: "ada", name: "Ada" },
  { id: "grace", name: "Grace" },
  { id: "lin", name: "Lin" },
  { id: "omar", name: "Omar" },
] as const;

/**
 * Durable application values.
 *
 * These are object *types* rather than interfaces on purpose: every one of them
 * is written into a durable State stream as a JSON record, and only a type
 * alias carries the implicit index signature that lets TypeScript accept it as
 * a `JsonValue` without an assertion.
 */
export const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
export const Identifier = Schema.String.check(Schema.isPattern(IDENTIFIER_PATTERN));
export const Prose = Schema.String.check(Schema.isPattern(/\S/, { title: "a non-blank string" }));
export const MemberId = Schema.Literals(TEAM.map((member) => member.id));
export type MemberId = typeof MemberId.Type;

export const CommentSchema = Schema.Struct({
  commentId: Identifier,
  authorId: Schema.NonEmptyString,
  body: Prose,
  at: Schema.NonEmptyString,
});
export type Comment = typeof CommentSchema.Type;

export const IssueDetailSchema = Schema.Struct({
  issueId: Identifier,
  issueKey: Identifier,
  projectId: Identifier,
  title: Prose,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  assigneeId: Schema.NullOr(Schema.NonEmptyString),
  comments: Schema.Array(CommentSchema),
  createdAt: Schema.NonEmptyString,
  updatedAt: Schema.NonEmptyString,
});
export type IssueDetail = typeof IssueDetailSchema.Type;

export const BoardRowSchema = Schema.Struct({
  issueId: Identifier,
  issueKey: Identifier,
  title: Prose,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  assigneeId: Schema.NullOr(Schema.NonEmptyString),
  commentCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  updatedAt: Schema.NonEmptyString,
});
export type BoardRow = typeof BoardRowSchema.Type;

export const ProjectSchema = Schema.Struct({
  projectId: Identifier,
  projectKey: Identifier,
  name: Prose,
});
export type Project = typeof ProjectSchema.Type;

export const decodeIssueDetail = Schema.decodeUnknownSync(IssueDetailSchema);
export const decodeBoardRow = Schema.decodeUnknownSync(BoardRowSchema);
export const decodeProject = Schema.decodeUnknownSync(ProjectSchema);

export const ISSUE_DETAIL_COLLECTION = "issue-detail";
export const BOARD_ROW_COLLECTION = "board-issue";
export const PROJECT_COLLECTION = "project";

export function boardRow(detail: IssueDetail): BoardRow {
  return {
    issueId: detail.issueId,
    issueKey: detail.issueKey,
    title: detail.title,
    status: detail.status,
    priority: detail.priority,
    assigneeId: detail.assigneeId,
    commentCount: detail.comments.length,
    updatedAt: detail.updatedAt,
  };
}

/** Stable, inspectable stream names. Identity and stream id are kept equal. */
export const streamNames = {
  projects: (workspaceId: string) => `workspaces/${workspaceId}/projects`,
  issueEvents: (workspaceId: string, issueId: string) =>
    `workspaces/${workspaceId}/issues/${issueId}/events`,
  issueDetail: (workspaceId: string, issueId: string) =>
    `workspaces/${workspaceId}/issues/${issueId}/detail`,
  membership: (workspaceId: string, projectId: string) =>
    `workspaces/${workspaceId}/projects/${projectId}/membership`,
  board: (workspaceId: string, projectId: string) =>
    `workspaces/${workspaceId}/projects/${projectId}/board`,
} as const;

export type TeamMemberId = (typeof TEAM)[number]["id"];

export function isKnownMember(userId: string): userId is TeamMemberId {
  return TEAM.some((member) => member.id === userId);
}
