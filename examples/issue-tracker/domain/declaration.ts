/**
 * The user declaration this slice makes executable.
 *
 * This module is the whole "user code" surface of the vertical slice: one
 * source, one reducer, one view, one sink. Everything else in `server/` is the
 * runtime that carries it. Read it alongside the plan artefact — if a later
 * change makes `issues.plan` hash differently, the maintained state is a
 * different thing and the host must say so.
 */
import { defineStateSink, STATE_SINK_ERROR_TAGS } from "@streamsy/state-sink";
import { decodeIdentifier, decodeIssueRow, IssueEvent, IssueRow } from "./issue.ts";
import type { IssueEvent as IssueEventType, IssueRow as IssueRowType } from "./issue.ts";
import {
  factSourceMode,
  from,
  reducer,
  selectors,
  source,
  stateSourceMode,
  view,
} from "@streamsy/views";
import {
  LabelRow,
  ProjectRow,
  UserRow,
  WorkspaceMetadataRow,
  type LabelRow as LabelRowType,
  type ProjectRow as ProjectRowType,
  type UserRow as UserRowType,
  type WorkspaceMetadataRow as WorkspaceMetadataRowType,
} from "./catalog.ts";

/** Selectors over one canonical fact, the fold state, and the maintained row. */
const x = selectors<IssueEventType, IssueEventType, IssueRowType>();
/** Selectors over the maintained relation's own rows. */
const out = selectors<IssueRowType>();

export const issueEvents = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  schemaRef: { name: "issue-tracker.IssueEvent", version: 1 },
  partitionBy: x.row.workspaceId,
  mode: factSourceMode(x.row.eventId, x.row.sequence),
});

const project = selectors<ProjectRowType>();
const user = selectors<UserRowType>();
const label = selectors<LabelRowType>();
const workspace = selectors<WorkspaceMetadataRowType>();

export const projects = source("issue-tracker.projects", {
  schema: ProjectRow,
  schemaRef: { name: "issue-tracker.ProjectRow", version: 1 },
  partitionBy: project.row.workspaceId,
  mode: stateSourceMode(project.row.projectId),
});

export const users = source("issue-tracker.users", {
  schema: UserRow,
  schemaRef: { name: "issue-tracker.UserRow", version: 1 },
  partitionBy: user.row.workspaceId,
  mode: stateSourceMode(user.row.userId),
});

export const labels = source("issue-tracker.labels", {
  schema: LabelRow,
  schemaRef: { name: "issue-tracker.LabelRow", version: 1 },
  partitionBy: label.row.workspaceId,
  mode: stateSourceMode(label.row.labelId),
});

export const workspaceMetadata = source("issue-tracker.workspace-metadata", {
  schema: WorkspaceMetadataRow,
  schemaRef: { name: "issue-tracker.WorkspaceMetadataRow", version: 1 },
  partitionBy: workspace.row.workspaceId,
  mode: stateSourceMode(workspace.row.workspaceId),
});

export const issueLifecycle = reducer(
  { name: "issue-tracker.issue-lifecycle", version: 1 },
  {
    state: IssueRow,
    stateRef: { name: "issue-tracker.IssueRow", version: 1 },
    input: IssueEvent,
    discriminator: "type",
    evolve: {
      IssueCreated: (e) => ({
        issueId: e.event.issueId,
        workspaceId: e.event.workspaceId,
        projectId: e.event.projectId,
        title: e.event.title,
        status: e.event.status,
        updatedAt: e.event.occurredAt,
      }),
      IssueStatusChanged: (e) => ({
        status: e.event.status,
        updatedAt: e.event.occurredAt,
      }),
    },
  },
);

export const issues = view(
  "issue-tracker.issues",
  {
    schema: IssueRow,
    schemaRef: { name: "issue-tracker.IssueRow", version: 1 },
    key: out.row.issueId,
  },
  from(issueEvents).reduceByKey({
    key: x.row.issueId,
    order: x.row.sequence,
    reducer: issueLifecycle,
  }),
);

export const boardIssues = defineStateSink({
  name: "issue-tracker.board-issues",
  from: issues,
  row: { decode: decodeIssueRow },
  key: "issueId",
  route: "/state/workspaces/:workspaceId/issues",
  params: { workspaceId: { decode: decodeIdentifier } },
  collection: { name: "issues", type: "issue", primaryKey: "issueId" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
  auth: { policy: "issue-tracker.workspace", required: "issue-tracker:workspace" },
  errors: STATE_SINK_ERROR_TAGS,
});

/** Durable stream names. Identity and stream id are kept equal so lineage reads by inspection. */
export const streamNames = {
  issueEvents: (workspaceId: string): string => `workspaces/${workspaceId}/issue-events`,
  boardState: (workspaceId: string): string => `state/workspaces/${workspaceId}/issues`,
  projects: (workspaceId: string): string => `state/workspaces/${workspaceId}/projects`,
  users: (workspaceId: string): string => `state/workspaces/${workspaceId}/users`,
  labels: (workspaceId: string): string => `state/workspaces/${workspaceId}/labels`,
  metadata: (workspaceId: string): string => `state/workspaces/${workspaceId}/metadata`,
} as const;
