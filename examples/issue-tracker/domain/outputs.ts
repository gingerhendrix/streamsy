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
export const WorkspaceState = Schema.Struct({
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
export type WorkspaceState = typeof WorkspaceState.Type;
export const initialWorkspace = (workspaceId: string): WorkspaceState => ({
  workspaceId,
  issueCount: 0,
  doneCount: 0,
  issues: [],
  memberships: [],
  projects: [],
  users: [],
  labels: [],
});
export const WorkspaceSummary = Schema.Struct({
  workspaceId: Identifier,
  issueCount: Sequence,
  doneCount: Sequence,
  labelCount: Sequence,
  projectCount: Sequence,
});
export type WorkspaceSummary = typeof WorkspaceSummary.Type;

const group = <A>(rows: ReadonlyArray<A>, key: (row: A) => string) => {
  const result = new Map<string, A[]>();
  for (const row of rows) {
    const id = key(row);
    const bucket = result.get(id);
    if (bucket) bucket.push(row);
    else result.set(id, [row]);
  }
  return result;
};

/** Rebuilt in linear time; row builders visit only the keys touched by a unit. */
export const indexWorkspace = (state: WorkspaceState) => ({
  issues: new Map(state.issues.map((row) => [row.issueId, row])),
  projects: new Map(state.projects.map((row) => [row.projectId, row])),
  users: new Map(state.users.map((row) => [row.userId, row])),
  labels: new Map(state.labels.map((row) => [row.labelId, row])),
  membershipsByIssue: group(
    state.memberships.filter((row) => row.attached),
    (row) => row.issueId,
  ),
  membershipsByLabel: group(
    state.memberships.filter((row) => row.attached),
    (row) => row.labelId,
  ),
  issuesByProject: group(state.issues, (row) => row.projectId),
  issuesByUser: group(
    state.issues.filter((row) => row.assigneeId !== undefined),
    (row) => row.assigneeId!,
  ),
});
export type WorkspaceIndex = ReturnType<typeof indexWorkspace>;
export const boardCard = (index: WorkspaceIndex, issue: IssueRow): ProjectBoardCard => ({
  ...issue,
  labelIds: (index.membershipsByIssue.get(issue.issueId) ?? []).map((row) => row.labelId).sort(),
  projectName: index.projects.get(issue.projectId)?.name ?? null,
  assigneeName:
    issue.assigneeId === undefined ? null : (index.users.get(issue.assigneeId)?.name ?? null),
});
export const labelCount = (index: WorkspaceIndex, label: LabelRow): LabelCountRow => ({
  workspaceId: label.workspaceId,
  labelId: label.labelId,
  name: label.name,
  count: (index.membershipsByLabel.get(label.labelId) ?? []).filter((row) =>
    index.issues.has(row.issueId),
  ).length,
});
