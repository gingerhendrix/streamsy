import { StreamRoute } from "@streamsy/core";
import { Output, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect } from "effect";
import { Identifier, foldIssue, type IssueEvent } from "../domain/issue.ts";
import {
  ProjectBoardCard,
  LabelCountRow,
  IssueTransition,
  WorkspaceSummary,
  initialSummary,
  boardCards,
  countLabels,
} from "../domain/outputs.ts";
import { routes } from "./streams.ts";

const target = (name: string) =>
  StreamRoute.bytes(`issue-tracker/:workspaceId/outputs/${name}`, {
    params: { workspaceId: Identifier },
  });

/** Named family members are Projection.outputs declarations, separate from issueRows. */
export const tracker = Projection.family({
  id: "issue-tracker",
  params: { workspaceId: Identifier },
  inputs: routes,
  outputs: {
    board: Output.rows(ProjectBoardCard, { key: "issueId", stream: target("board") }),
    labelCounts: Output.rows(LabelCountRow, { key: "labelId", stream: target("label-counts") }),
    transitions: Output.stream(IssueTransition, { stream: target("transitions") }),
    summary: Output.value(WorkspaceSummary),
  },
  process: Projection.fold(
    (params) => initialSummary(params.workspaceId!),
    (previous, batch) =>
      Effect.gen(function* () {
        const issues = new Map(previous.issues.map((row) => [row.issueId, row]));
        const memberships = new Map(previous.memberships.map((row) => [row.membershipId, row]));
        const projects = new Map(previous.projects.map((row) => [row.projectId, row]));
        const users = new Map(previous.users.map((row) => [row.userId, row]));
        const labels = new Map(previous.labels.map((row) => [row.labelId, row]));
        const transitions: IssueEvent[] = [];
        for (const event of batch.events.items) {
          const before = issues.get(event.issueId);
          const after = foldIssue(before, event);
          if (after === undefined || after === before) continue;
          issues.set(event.issueId, after);
          transitions.push(event);
        }
        for (const event of batch.labelEvents.items) {
          const before = memberships.get(event.membershipId);
          if (before !== undefined && before.sequence >= event.sequence) continue;
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
        // Match the SQL reader's explicit refusal: never silently skip an ambiguous delete.
        for (const change of [
          ...batch.projects.items,
          ...batch.users.items,
          ...batch.labels.items,
        ]) {
          if (!("value" in change) && change.old_value === undefined)
            return yield* new ProjectionFault({
              phase: "process",
              reason: "invalid-output",
              message: `Catalog delete ${change.type}/${change.key} requires old_value`,
            });
        }
        for (const change of batch.projects.items) {
          if ("value" in change) projects.set(change.key, change.value);
          else projects.delete(change.key);
        }
        for (const change of batch.users.items) {
          if ("value" in change) users.set(change.key, change.value);
          else users.delete(change.key);
        }
        for (const change of batch.labels.items) {
          if ("value" in change) labels.set(change.key, change.value);
          else labels.delete(change.key);
        }
        const state: WorkspaceSummary = {
          workspaceId: previous.workspaceId,
          issueCount: issues.size,
          doneCount: [...issues.values()].filter((row) => row.status === "done").length,
          issues: [...issues.values()],
          memberships: [...memberships.values()],
          projects: [...projects.values()],
          users: [...users.values()],
          labels: [...labels.values()],
        };
        const oldCards = new Map(boardCards(previous).map((row) => [row.issueId, row]));
        const oldCounts = new Map(countLabels(previous).map((row) => [row.labelId, row]));
        const counts = countLabels(state);
        return {
          state,
          board: boardCards(state)
            .filter((row) => JSON.stringify(row) !== JSON.stringify(oldCards.get(row.issueId)))
            .map(Output.upsert),
          labelCounts: [
            ...counts
              .filter((row) => JSON.stringify(row) !== JSON.stringify(oldCounts.get(row.labelId)))
              .map(Output.upsert),
            ...[...oldCounts.keys()].filter((key) => !labels.has(key)).map(Output.remove),
          ],
          transitions,
        };
      }),
  ),
});
