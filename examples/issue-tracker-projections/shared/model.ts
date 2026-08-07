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

export const ISSUE_PRIORITIES = ["low", "medium", "high", "urgent"] as const;
export type IssuePriority = (typeof ISSUE_PRIORITIES)[number];

export const TEAM = [
  { id: "ada", name: "Ada" },
  { id: "grace", name: "Grace" },
  { id: "lin", name: "Lin" },
  { id: "omar", name: "Omar" },
] as const;

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

export function isKnownMember(userId: string): boolean {
  return TEAM.some((member) => member.id === userId);
}
