import { Streamsy } from "@streamsy/core";
import { Output, Projection } from "@streamsy/projection";
import { Effect } from "effect";
import { Identifier, foldIssue, type IssueEvent } from "../domain/issue.ts";
import {
  ProjectBoardCard,
  LabelCountRow,
  IssueTransition,
  WorkspaceState,
  initialWorkspace,
  indexWorkspace,
  boardCard,
  labelCount,
} from "../domain/outputs.ts";
import { checkCatalogWorkspace } from "./catalog.ts";
import { routes } from "./streams.ts";

export class Board extends Streamsy.bytes<Board>()(
  "IssueTracker/Board",
  "issue-tracker/:workspaceId/outputs/board",
  {
    params: { workspaceId: Identifier },
  },
) {}
export class LabelCounts extends Streamsy.bytes<LabelCounts>()(
  "IssueTracker/LabelCounts",
  "issue-tracker/:workspaceId/outputs/label-counts",
  { params: { workspaceId: Identifier } },
) {}
export class Transitions extends Streamsy.bytes<Transitions>()(
  "IssueTracker/Transitions",
  "issue-tracker/:workspaceId/outputs/transitions",
  { params: { workspaceId: Identifier } },
) {}
export const outputStreams = [Board, LabelCounts, Transitions] as const;

/** Declared outputs for one workspace, separate from the SQL rows of issueRows. */
export const tracker = Projection.outputs({
  id: "issue-tracker",
  params: { workspaceId: Identifier },
  inputs: routes,
  outputs: {
    board: Output.rows(ProjectBoardCard, { key: "issueId", stream: Board }),
    labelCounts: Output.rows(LabelCountRow, { key: "labelId", stream: LabelCounts }),
    transitions: Output.stream(IssueTransition, { stream: Transitions }),
  },
  state: {
    schema: WorkspaceState,
    initial: ({ workspaceId }) => initialWorkspace(workspaceId),
  },
  process: (previous, batch) =>
    Effect.gen(function* () {
      const issues = new Map(previous.issues.map((row) => [row.issueId, row]));
      const memberships = new Map(previous.memberships.map((row) => [row.membershipId, row]));
      const projects = new Map(previous.projects.map((row) => [row.projectId, row]));
      const users = new Map(previous.users.map((row) => [row.userId, row]));
      const labels = new Map(previous.labels.map((row) => [row.labelId, row]));
      const beforeIndex = indexWorkspace(previous);
      const touchedIssues = new Set<string>();
      const touchedLabels = new Set<string>();
      const touchedProjects = new Set<string>();
      const touchedUsers = new Set<string>();
      const transitions: IssueEvent[] = [];
      for (const event of batch.events.items) {
        const before = issues.get(event.issueId);
        const after = foldIssue(before, event);
        if (after === undefined || after === before) continue;
        issues.set(event.issueId, after);
        touchedIssues.add(event.issueId);
        transitions.push(event);
      }
      for (const event of batch.labelEvents.items) {
        const before = memberships.get(event.membershipId);
        if (before !== undefined && before.sequence >= event.sequence) continue;
        touchedIssues.add(event.issueId);
        touchedLabels.add(event.labelId);
        if (before !== undefined) {
          touchedIssues.add(before.issueId);
          touchedLabels.add(before.labelId);
        }
        memberships.set(event.membershipId, {
          membershipId: event.membershipId,
          workspaceId: event.workspaceId,
          issueId: event.issueId,
          labelId: event.labelId,
          attached: event.type === "LabelAttached",
          sequence: event.sequence,
          updatedAt: event.occurredAt,
        });
      }
      for (const change of [...batch.projects.items, ...batch.users.items, ...batch.labels.items]) {
        if ("value" in change)
          yield* checkCatalogWorkspace(
            previous.workspaceId,
            change.value.workspaceId,
            change.type,
            change.key,
          );
      }
      for (const change of batch.projects.items) {
        touchedProjects.add(change.key);
        if ("value" in change) projects.set(change.key, change.value);
        else projects.delete(change.key);
      }
      for (const change of batch.users.items) {
        touchedUsers.add(change.key);
        if ("value" in change) users.set(change.key, change.value);
        else users.delete(change.key);
      }
      for (const change of batch.labels.items) {
        touchedLabels.add(change.key);
        if ("value" in change) labels.set(change.key, change.value);
        else labels.delete(change.key);
      }
      const state: WorkspaceState = {
        workspaceId: previous.workspaceId,
        issueCount: issues.size,
        doneCount: [...issues.values()].filter((row) => row.status === "done").length,
        issues: [...issues.values()],
        memberships: [...memberships.values()],
        projects: [...projects.values()],
        users: [...users.values()],
        labels: [...labels.values()],
      };
      const afterIndex = indexWorkspace(state);
      for (const key of touchedProjects) {
        for (const issue of beforeIndex.issuesByProject.get(key) ?? [])
          touchedIssues.add(issue.issueId);
        for (const issue of afterIndex.issuesByProject.get(key) ?? [])
          touchedIssues.add(issue.issueId);
      }
      for (const key of touchedUsers) {
        for (const issue of beforeIndex.issuesByUser.get(key) ?? [])
          touchedIssues.add(issue.issueId);
        for (const issue of afterIndex.issuesByUser.get(key) ?? [])
          touchedIssues.add(issue.issueId);
      }
      for (const key of touchedIssues) {
        for (const row of beforeIndex.membershipsByIssue.get(key) ?? [])
          touchedLabels.add(row.labelId);
        for (const row of afterIndex.membershipsByIssue.get(key) ?? [])
          touchedLabels.add(row.labelId);
      }
      // Preserve insertion order and builder field order, including for JSON comparisons.
      const board: Output.Change<ProjectBoardCard>[] = [];
      for (const issue of state.issues) {
        if (!touchedIssues.has(issue.issueId)) continue;
        const row = boardCard(afterIndex, issue);
        const oldIssue = beforeIndex.issues.get(issue.issueId);
        const oldRow = oldIssue === undefined ? undefined : boardCard(beforeIndex, oldIssue);
        if (JSON.stringify(row) !== JSON.stringify(oldRow)) board.push(Output.upsert(row));
      }
      const labelCounts: Output.Change<LabelCountRow>[] = [];
      for (const label of state.labels) {
        if (!touchedLabels.has(label.labelId)) continue;
        const row = labelCount(afterIndex, label);
        const oldLabel = beforeIndex.labels.get(label.labelId);
        const oldRow = oldLabel === undefined ? undefined : labelCount(beforeIndex, oldLabel);
        if (JSON.stringify(row) !== JSON.stringify(oldRow)) labelCounts.push(Output.upsert(row));
      }
      for (const label of previous.labels) {
        if (!labels.has(label.labelId)) labelCounts.push(Output.remove(label.labelId));
      }
      return { state, board, labelCounts, transitions };
    }),
});
