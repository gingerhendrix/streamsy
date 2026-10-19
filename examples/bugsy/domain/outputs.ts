import { Array, Order, Schema } from "effect";
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
  issues: new Map(Array.map(state.issues, (row) => [row.issueId, row] as const)),
  projects: new Map(Array.map(state.projects, (row) => [row.projectId, row] as const)),
  users: new Map(Array.map(state.users, (row) => [row.userId, row] as const)),
  labels: new Map(Array.map(state.labels, (row) => [row.labelId, row] as const)),
  membershipsByIssue: group(
    Array.filter(state.memberships, (row) => row.attached),
    (row) => row.issueId,
  ),
  membershipsByLabel: group(
    Array.filter(state.memberships, (row) => row.attached),
    (row) => row.labelId,
  ),
  issuesByProject: group(state.issues, (row) => row.projectId),
  issuesByUser: group(
    Array.filter(state.issues, (row) => row.assigneeId !== undefined),
    (row) => row.assigneeId!,
  ),
});

export type WorkspaceIndex = ReturnType<typeof indexWorkspace>;

export const boardCard = (index: WorkspaceIndex, issue: IssueRow): ProjectBoardCard => ({
  ...issue,
  labelIds: Array.sort(
    Array.map(index.membershipsByIssue.get(issue.issueId) ?? [], (row) => row.labelId),
    Order.String,
  ),
  projectName: index.projects.get(issue.projectId)?.name ?? null,
  assigneeName:
    issue.assigneeId === undefined ? null : (index.users.get(issue.assigneeId)?.name ?? null),
});

export const labelCount = (index: WorkspaceIndex, label: LabelRow): LabelCountRow => ({
  workspaceId: label.workspaceId,
  labelId: label.labelId,
  name: label.name,
  count: Array.filter(index.membershipsByLabel.get(label.labelId) ?? [], (row) =>
    index.issues.has(row.issueId),
  ).length,
});
