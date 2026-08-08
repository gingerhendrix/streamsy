/**
 * Wire contract shared by the local host, the Worker, and the browser.
 *
 * Request bodies are defined once as Effect Schemas in `./requests.ts` and
 * re-exported here as types. The re-export is type-only, so the browser bundle
 * still contains no Effect runtime.
 */
import type { BoardRow, IssueDetail, Project } from "./domain.ts";

export type { CreateIssueRequest, CreateProjectRequest, IssueCommandRequest } from "./requests.ts";

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

/**
 * What one bounded projection pass did.
 *
 * `caught-up` is the only outcome that permits a `Synced` claim. `deferred`
 * means more bounded work remains and repair or the wake consumer will carry
 * it; `faulted` means this pass cannot make progress without intervention.
 */
export type ProjectionOutcome = "caught-up" | "deferred" | "faulted";

export interface ProjectionPassReport {
  readonly label: "issue-detail" | "project-board";
  /** The exact kernel status, or the tag of a typed mesh error. */
  readonly status: string;
  readonly outcome: ProjectionOutcome;
  readonly detail?: string;
}

/** A read-only lineage probe for one accepted acknowledgement. */
export interface CoverageResponse {
  readonly issueId: string;
  readonly projectId: string;
  readonly coverage: CoverageReport;
  /** Always empty: a probe reads lineage and runs no projection work. */
  readonly projections: readonly ProjectionPassReport[];
}

export interface MutationResponse {
  readonly commandId: string;
  readonly issueId: string;
  readonly projectId: string;
  /** The exact accepted source acknowledgement for this command. */
  readonly ack: { readonly stream: string; readonly position: string };
  readonly reconciled: boolean;
  readonly coverage: CoverageReport;
  /** Classified result of every projection pass this request attempted. */
  readonly projections: readonly ProjectionPassReport[];
  readonly detail: IssueDetail | null;
}

export interface RepairResponse {
  readonly repaired: readonly string[];
  readonly board: string;
  readonly projections: readonly ProjectionPassReport[];
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
