import { Schema } from "effect";
import { LabelRow, ProjectRow, UserRow } from "./catalog.ts";
import { Identifier, IssueEvent, IssueLabelRow, IssueRow, Sequence } from "./issue.ts";

export const ProjectBoardCard = Schema.Struct({
  ...IssueRow.fields,
  labelIds: Schema.Array(Identifier),
  projectName: Schema.NullOr(Schema.String),
  assigneeName: Schema.NullOr(Schema.String),
});
export type ProjectBoardCard = typeof ProjectBoardCard.Type;
export const LabelCountRow = Schema.Struct({
  workspaceId: Identifier,
  labelId: Identifier,
  name: Schema.String,
  count: Sequence,
});
export type LabelCountRow = typeof LabelCountRow.Type;
// Accepted issue facts retain their stable event identity in the transition feed.
export const IssueTransition = IssueEvent;
export const WorkspaceSummary = Schema.Struct({
  workspaceId: Identifier,
  issueCount: Sequence,
  doneCount: Sequence,
  // The value is also the replayable fold state: retain source rows and tombstones.
  issues: Schema.Array(IssueRow),
  memberships: Schema.Array(IssueLabelRow),
  projects: Schema.Array(ProjectRow),
  users: Schema.Array(UserRow),
  labels: Schema.Array(LabelRow),
});
export type WorkspaceSummary = typeof WorkspaceSummary.Type;
export const initialSummary = (workspaceId: string): WorkspaceSummary => ({
  workspaceId,
  issueCount: 0,
  doneCount: 0,
  issues: [],
  memberships: [],
  projects: [],
  users: [],
  labels: [],
});
export const boardCards = (state: WorkspaceSummary): ReadonlyArray<ProjectBoardCard> =>
  state.issues.map((issue) => ({
    ...issue,
    labelIds: state.memberships
      .filter((row) => row.issueId === issue.issueId && row.attached)
      .map((row) => row.labelId)
      .sort(),
    projectName: state.projects.find((row) => row.projectId === issue.projectId)?.name ?? null,
    assigneeName: state.users.find((row) => row.userId === issue.assigneeId)?.name ?? null,
  }));
export const countLabels = (state: WorkspaceSummary): ReadonlyArray<LabelCountRow> =>
  state.labels.map((label) => ({
    workspaceId: state.workspaceId,
    labelId: label.labelId,
    name: label.name,
    count: state.memberships.filter(
      (row) =>
        row.labelId === label.labelId &&
        row.attached &&
        state.issues.some((issue) => issue.issueId === row.issueId),
    ).length,
  }));
