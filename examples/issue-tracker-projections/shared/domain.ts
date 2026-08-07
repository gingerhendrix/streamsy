/**
 * Application-owned domain for the projection issue tracker.
 *
 * This module owns schemas, pure transitions, stream naming, and State fact
 * shapes. It owns no recovery, lineage, or commit behaviour: those belong to
 * the experimental mesh kernels.
 */
import { Schema } from "effect";

export const ISSUE_STATUSES = ["backlog", "in-progress", "done"] as const;
export type IssueStatus = (typeof ISSUE_STATUSES)[number];

export const ISSUE_PRIORITIES = ["low", "medium", "high", "urgent"] as const;
export type IssuePriority = (typeof ISSUE_PRIORITIES)[number];

export const TEAM = [
  { id: "ada", name: "Ada" },
  { id: "grace", name: "Grace" },
  { id: "lin", name: "Lin" },
  { id: "omar", name: "Omar" },
] as const;

export const IssueStatusSchema = Schema.Literals(ISSUE_STATUSES);
export const IssuePrioritySchema = Schema.Literals(ISSUE_PRIORITIES);

const Base = {
  commandId: Schema.NonEmptyString,
  at: Schema.NonEmptyString,
};

export const IssueCreated = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueCreated"),
  issueId: Schema.NonEmptyString,
  issueKey: Schema.NonEmptyString,
  projectId: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  status: IssueStatusSchema,
  priority: IssuePrioritySchema,
  creatorId: Schema.NonEmptyString,
});

export const IssueRenamed = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueRenamed"),
  title: Schema.NonEmptyString,
});

export const IssueStatusChanged = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueStatusChanged"),
  status: IssueStatusSchema,
});

export const IssuePriorityChanged = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssuePriorityChanged"),
  priority: IssuePrioritySchema,
});

export const IssueAssigned = Schema.Struct({
  ...Base,
  type: Schema.Literal("IssueAssigned"),
  assigneeId: Schema.NullOr(Schema.NonEmptyString),
});

export const CommentAdded = Schema.Struct({
  ...Base,
  type: Schema.Literal("CommentAdded"),
  commentId: Schema.NonEmptyString,
  authorId: Schema.NonEmptyString,
  body: Schema.NonEmptyString,
});

export const IssueEvent = Schema.Union([
  IssueCreated,
  IssueRenamed,
  IssueStatusChanged,
  IssuePriorityChanged,
  IssueAssigned,
  CommentAdded,
]);
export type IssueEvent = Schema.Schema.Type<typeof IssueEvent>;

export const ProjectMembershipFact = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("IssueJoined"),
    issueId: Schema.NonEmptyString,
    /** Detail-stream position the board should start from, when known. */
    from: Schema.NullOr(Schema.NonEmptyString),
  }),
  Schema.Struct({ type: Schema.Literal("IssueLeft"), issueId: Schema.NonEmptyString }),
]);
export type ProjectMembershipFact = Schema.Schema.Type<typeof ProjectMembershipFact>;

export interface Comment {
  readonly commentId: string;
  readonly authorId: string;
  readonly body: string;
  readonly at: string;
}

export interface IssueDetail {
  readonly issueId: string;
  readonly issueKey: string;
  readonly projectId: string;
  readonly title: string;
  readonly status: IssueStatus;
  readonly priority: IssuePriority;
  readonly assigneeId: string | null;
  readonly comments: readonly Comment[];
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface BoardRow {
  readonly issueId: string;
  readonly issueKey: string;
  readonly title: string;
  readonly status: IssueStatus;
  readonly priority: IssuePriority;
  readonly assigneeId: string | null;
  readonly commentCount: number;
  readonly updatedAt: string;
}

export interface Project {
  readonly projectId: string;
  readonly projectKey: string;
  readonly name: string;
}

export const ISSUE_DETAIL_COLLECTION = "issue-detail";
export const BOARD_ROW_COLLECTION = "board-issue";
export const PROJECT_COLLECTION = "project";

/** Fold one issue event into detail state. A missing creation is a domain fault. */
export function evolveIssue(current: IssueDetail | undefined, event: IssueEvent): IssueDetail {
  if (event.type === "IssueCreated") {
    if (current !== undefined) return current;
    return {
      issueId: event.issueId,
      issueKey: event.issueKey,
      projectId: event.projectId,
      title: event.title,
      status: event.status,
      priority: event.priority,
      assigneeId: null,
      comments: [],
      createdAt: event.at,
      updatedAt: event.at,
    };
  }
  if (current === undefined) {
    throw new TypeError(`Issue event ${event.type} arrived before IssueCreated`);
  }
  const touched = { ...current, updatedAt: event.at };
  switch (event.type) {
    case "IssueRenamed":
      return { ...touched, title: event.title };
    case "IssueStatusChanged":
      return { ...touched, status: event.status };
    case "IssuePriorityChanged":
      return { ...touched, priority: event.priority };
    case "IssueAssigned":
      return { ...touched, assigneeId: event.assigneeId };
    case "CommentAdded":
      return current.comments.some((comment) => comment.commentId === event.commentId)
        ? touched
        : {
            ...touched,
            comments: [
              ...current.comments,
              {
                commentId: event.commentId,
                authorId: event.authorId,
                body: event.body,
                at: event.at,
              },
            ],
          };
  }
}

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

const SEGMENT = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

/** Reject identifiers that would break stream paths or workspace isolation. */
export function assertIdentifier(value: string, name: string): string {
  if (!SEGMENT.test(value)) throw new TypeError(`${name} must match ${String(SEGMENT)}`);
  return value;
}

export function isKnownMember(userId: string): boolean {
  return TEAM.some((member) => member.id === userId);
}
