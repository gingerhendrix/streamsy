import { Issue } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Projection } from "@streamsy/projection";
import { Array, Match, Record, Schema } from "effect";
import { issueAfter, type IssueEvent, type WorkspaceEvent } from "./events.ts";
import { WorkspaceEvents } from "./streams.ts";

/**
 * What a command needs to know about one workspace: which projects exist, and
 * each issue row. The rules read the project and the status of an issue. A
 * write answers with the whole row, so the view keeps the row.
 */
export const CommandView = Schema.Struct({
  projectIds: Schema.Array(Schema.String),
  issues: Schema.Record(Schema.String, Issue),
});

export interface CommandView extends Schema.Schema.Type<typeof CommandView> {}

export const emptyView: CommandView = { projectIds: [], issues: {} };

/** The view after one event. Comments change nothing that a rule reads. */
export function applyEvent(view: CommandView, event: WorkspaceEvent): CommandView {
  return Match.valueTags(event, {
    ProjectCreated: ({ projectId }) => ({
      projectIds: [...view.projectIds, projectId],
      issues: view.issues,
    }),
    IssueCreated: (created) => withIssue(view, created),
    IssueRenamed: (renamed) => withIssue(view, renamed),
    IssueStatusChanged: (changed) => withIssue(view, changed),
    CommentAdded: () => view,
  });
}

function withIssue(view: CommandView, event: IssueEvent): CommandView {
  const row = issueAfter(view.issues[event.issueId], event);

  return row === undefined
    ? view
    : { projectIds: view.projectIds, issues: { ...view.issues, [row.id]: row } };
}

/**
 * The command view of each workspace, folded from its event stream. The default
 * Commit can save the value and event offset in one checkpoint record. This app
 * uses current through decide, so commands fold in memory without a writer.
 */
export const commandView = Projection.fold({
  id: "issue-tracker-commands",
  params: { workspaceId: Schema.String },
  inputs: { events: WorkspaceEvents },
  state: { schema: CommandView, initial: emptyView },
  fold: (view, { item }) => applyEvent(view, item),
});

export const hasProject = (view: CommandView, projectId: string): boolean =>
  view.projectIds.includes(projectId);

export const findIssue = (view: CommandView, issueId: string): Issue | undefined =>
  view.issues[issueId];

/** How many issues of the project are `in_progress` in this view. */
export const countInProgress = (view: CommandView, projectId: string): number =>
  Array.filter(
    Record.values(view.issues),
    (issue) => issue.projectId === projectId && issue.status === "in_progress",
  ).length;
