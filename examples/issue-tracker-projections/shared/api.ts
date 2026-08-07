/** Wire contract shared by the local host, the Worker, and the browser. */
import type { BoardRow, IssueDetail, IssuePriority, IssueStatus, Project } from "./domain.ts";

export interface HopReport {
  readonly label: "issue-detail" | "project-board";
  readonly source: string;
  readonly through: string | null;
  readonly output: string | null;
}

export interface CoverageReport {
  readonly status: "proven" | "not-yet" | "incomparable";
  readonly blockedAt?: string;
  readonly ack: { readonly stream: string; readonly position: string };
  readonly hops: readonly HopReport[];
}

export interface MutationResponse {
  readonly commandId: string;
  readonly issueId: string;
  readonly projectId: string;
  /** The exact accepted source acknowledgement for this command. */
  readonly ack: { readonly stream: string; readonly position: string };
  readonly reconciled: boolean;
  readonly coverage: CoverageReport;
  readonly detail: IssueDetail | null;
}

export interface CreateIssueRequest {
  readonly commandId: string;
  readonly issueId: string;
  readonly projectId: string;
  readonly title: string;
  readonly priority?: IssuePriority;
  readonly status?: IssueStatus;
  readonly creatorId?: string;
}

export type IssueCommandRequest =
  | { readonly commandId: string; readonly type: "rename"; readonly title: string }
  | { readonly commandId: string; readonly type: "status"; readonly status: IssueStatus }
  | { readonly commandId: string; readonly type: "priority"; readonly priority: IssuePriority }
  | { readonly commandId: string; readonly type: "assign"; readonly assigneeId: string | null }
  | {
      readonly commandId: string;
      readonly type: "comment";
      readonly commentId: string;
      readonly authorId: string;
      readonly body: string;
    };

export interface CreateProjectRequest {
  readonly projectId: string;
  readonly projectKey: string;
  readonly name: string;
}

export interface BoardResponse {
  readonly projectId: string;
  readonly boardStream: string;
  readonly rows: readonly BoardRow[];
}

export interface ProjectsResponse {
  readonly workspaceId: string;
  readonly projects: readonly Project[];
}

export interface HealthResponse {
  readonly status: "ok";
  readonly deployment: string;
  readonly schemaVersion: string;
  readonly host: "local" | "cloudflare";
}

export interface ApiError {
  readonly error: string;
  readonly detail?: string;
}
