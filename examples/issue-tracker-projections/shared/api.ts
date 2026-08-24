/** Canonical wire contracts shared by the local host, Worker, scripts, and browser. */
import { Schema } from "effect";
import { BoardRowSchema, IssueDetailSchema, ProjectSchema } from "./model.ts";

export type { CreateIssueRequest, CreateProjectRequest, IssueCommandRequest } from "./requests.ts";

const Ack = Schema.Struct({ stream: Schema.String, position: Schema.String });

export const HopReport = Schema.Struct({
  label: Schema.Literals(["issue-detail", "project-board"]),
  source: Schema.String,
  through: Schema.NullOr(Schema.String),
  output: Schema.NullOr(Schema.String),
});
export type HopReport = typeof HopReport.Type;

export const CoverageReport = Schema.Struct({
  status: Schema.Literals(["proven", "not-yet", "incomparable"]),
  blockedAt: Schema.optionalKey(Schema.String),
  ack: Ack,
  hops: Schema.Array(HopReport),
});
export type CoverageReport = typeof CoverageReport.Type;

export const ProjectionOutcome = Schema.Literals(["caught-up", "deferred", "faulted"]);
export type ProjectionOutcome = typeof ProjectionOutcome.Type;

export const ProjectionPassReport = Schema.Struct({
  label: Schema.Literals(["issue-detail", "project-board"]),
  status: Schema.String,
  outcome: ProjectionOutcome,
  detail: Schema.optionalKey(Schema.String),
});
export type ProjectionPassReport = typeof ProjectionPassReport.Type;

export const CoverageResponse = Schema.Struct({
  issueId: Schema.String,
  projectId: Schema.String,
  coverage: CoverageReport,
  projections: Schema.Array(ProjectionPassReport),
});
export type CoverageResponse = typeof CoverageResponse.Type;

export const MutationResponse = Schema.Struct({
  commandId: Schema.String,
  issueId: Schema.String,
  projectId: Schema.String,
  ack: Ack,
  reconciled: Schema.Boolean,
  coverage: CoverageReport,
  projections: Schema.Array(ProjectionPassReport),
  detail: Schema.NullOr(IssueDetailSchema),
});
export type MutationResponse = typeof MutationResponse.Type;

export const RepairResponse = Schema.Struct({
  repaired: Schema.Array(Schema.String),
  board: Schema.String,
  projections: Schema.Array(ProjectionPassReport),
});
export type RepairResponse = typeof RepairResponse.Type;

export const BoardResponse = Schema.Struct({
  projectId: Schema.String,
  boardStream: Schema.String,
  rows: Schema.Array(BoardRowSchema),
});
export type BoardResponse = typeof BoardResponse.Type;

export const ProjectsResponse = Schema.Struct({
  workspaceId: Schema.String,
  projects: Schema.Array(ProjectSchema),
});
export type ProjectsResponse = typeof ProjectsResponse.Type;

export const HealthResponse = Schema.Struct({
  status: Schema.Literal("ok"),
  deployment: Schema.String,
  schemaVersion: Schema.String,
  host: Schema.Literals(["local", "cloudflare"]),
});
export type HealthResponse = typeof HealthResponse.Type;

export const SeedResponse = Schema.Struct({
  workspaceId: Schema.String,
  projects: Schema.Array(Schema.String),
  issues: Schema.Array(Schema.String),
});
export type SeedResponse = typeof SeedResponse.Type;

export const ApiError = Schema.Struct({
  error: Schema.String,
  detail: Schema.optionalKey(Schema.String),
});
export type ApiError = typeof ApiError.Type;
