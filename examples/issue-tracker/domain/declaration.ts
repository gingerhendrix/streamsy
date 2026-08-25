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
import { decodeIdentifier, decodeProjectBoardCard, IssueEvent, IssueRow } from "./issue.ts";
import type { IssueEvent as IssueEventType, IssueRow as IssueRowType } from "./issue.ts";
import { from, reducer, selectors, source, view } from "@streamsy/views";
import { projectBoard } from "./views.ts";

/**
 * The four Durable State catalog sources are declared beside their row schemas
 * in `catalog.ts`, because one declaration there drives both their plan keys
 * and the host's schema/type/primary-key table. They are re-exported so this
 * module stays the whole declaration surface.
 */
export { labels, projects, users, workspaceMetadata } from "./catalog.ts";

/** Selectors over one canonical fact, the fold state, and the maintained row. */
const x = selectors<IssueEventType, IssueEventType, IssueRowType>();

export const issueEvents = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  schemaRef: { name: "issue-tracker.IssueEvent", version: 1 },
  partitionBy: x.row.workspaceId,
  key: "eventId",
  mode: "facts",
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
    key: "issueId",
  },
  from(issueEvents).reduceByKey({
    key: "issueId",
    reducer: issueLifecycle,
  }),
);

export const boardIssues = defineStateSink({
  name: "issue-tracker.board-issues",
  from: projectBoard,
  row: { decode: decodeProjectBoardCard },
  route: "/state/workspaces/:workspaceId/issues",
  params: { workspaceId: { decode: decodeIdentifier } },
  collection: { name: "issues", type: "issue" },
  protocol: {
    sessionVersion: 1,
    durableStateVersion: 1,
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
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
