/**
 * The user declaration this slice makes executable.
 *
 * This module is the whole "user code" surface of the vertical slice: one
 * source, one reducer, one view, one sink. Everything else in `server/` is the
 * runtime that carries it. Read it alongside the plan artefact — if a later
 * change makes `issues.plan` hash differently, the maintained state is a
 * different thing and the host must say so.
 */
import { IssueEvent, IssueRow } from "./issue.ts";
import type { IssueEvent as IssueEventType, IssueRow as IssueRowType } from "./issue.ts";
import { from, reducer, scope, source, stateSink, view } from "../views/dsl.ts";
import { selectors } from "../views/expression.ts";

/** Selectors over one canonical fact, the fold state, and the maintained row. */
const x = selectors<IssueEventType, IssueEventType, IssueRowType>();
/** Selectors over the maintained relation's own rows. */
const out = selectors<IssueRowType>();

export const issueEvents = source("issue-tracker.issue-events", {
  schema: IssueEvent,
  schemaRef: { name: "issue-tracker.IssueEvent", version: 1 },
  key: x.row.eventId,
  order: x.row.sequence,
  partitionBy: x.row.workspaceId,
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

export const boardIssues = stateSink("issue-tracker.board-issues", {
  from: issues,
  key: out.row.issueId,
  route: "/state/workspaces/:workspaceId/issues",
  params: ["workspaceId"],
  protocol: {
    transport: "durable-state",
    resume: true,
    fallback: "snapshot-then-live",
  },
  auth: scope("issue-tracker:workspace"),
});

/** Durable stream names. Identity and stream id are kept equal so lineage reads by inspection. */
export const streamNames = {
  issueEvents: (workspaceId: string): string => `workspaces/${workspaceId}/issue-events`,
  boardState: (workspaceId: string): string => `state/workspaces/${workspaceId}/issues`,
} as const;
