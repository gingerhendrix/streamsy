/**
 * Web-standard domain values shared by the browser, the local host, and the
 * Worker.
 *
 * This module has no dependencies at all. Effect Schemas and event transitions
 * live in `domain.ts`, which re-exports everything here; keeping them apart
 * stops the browser bundle from pulling the Effect runtime in.
 */

export const ISSUE_STATUSES = ["backlog", "in-progress", "done"] as const;
export type IssueStatus = (typeof ISSUE_STATUSES)[number];

/** True when an arbitrary string is one of the three board statuses. */
export function isIssueStatus(value: string): value is IssueStatus {
  return ISSUE_STATUSES.some((status) => status === value);
}

export const ISSUE_PRIORITIES = ["low", "medium", "high", "urgent"] as const;
export type IssuePriority = (typeof ISSUE_PRIORITIES)[number];

/** True when an arbitrary string is one of the four issue priorities. */
export function isIssuePriority(value: string): value is IssuePriority {
  return ISSUE_PRIORITIES.some((priority) => priority === value);
}

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
export type Comment = {
  readonly commentId: string;
  readonly authorId: string;
  readonly body: string;
  readonly at: string;
};

export type IssueDetail = {
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
};

export type BoardRow = {
  readonly issueId: string;
  readonly issueKey: string;
  readonly title: string;
  readonly status: IssueStatus;
  readonly priority: IssuePriority;
  readonly assigneeId: string | null;
  readonly commentCount: number;
  readonly updatedAt: string;
};

export type Project = {
  readonly projectId: string;
  readonly projectKey: string;
  readonly name: string;
};

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

const SEGMENT = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;

/** Reject identifiers that would break stream paths or workspace isolation. */
export function assertIdentifier(value: string, name: string): string {
  if (!SEGMENT.test(value)) throw new TypeError(`${name} must match ${String(SEGMENT)}`);
  return value;
}

/** True when `value` is usable as one stream path segment. */
export function isIdentifier(value: string): boolean {
  return SEGMENT.test(value);
}

export type TeamMemberId = (typeof TEAM)[number]["id"];

export function isKnownMember(userId: string): userId is TeamMemberId {
  return TEAM.some((member) => member.id === userId);
}
