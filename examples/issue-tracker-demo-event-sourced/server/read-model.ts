import { DurableState } from "@streamsy/core";
import { Issue } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Commit, Projection } from "@streamsy/projection";
import { Match, Schema } from "effect";
import {
  commentRow,
  issueAfter,
  projectRow,
  type IssueEvent,
  type WorkspaceEvent,
} from "./events.ts";
import { WorkspaceEvents, Workspaces } from "./streams.ts";

/** The issue rows by id. A rename or a status change needs the whole row to upsert. */
export const IssueRows = Schema.Record(Schema.String, Issue);
export type IssueRows = typeof IssueRows.Type;

export type RowChange = DurableState.Change<typeof Workspaces.collections>;

/**
 * Each change carries the event's time and txid, so `awaitTxId` in the browser
 * confirms a write when its row lands, as it does in the base app.
 */
const headers = (event: WorkspaceEvent) => ({
  headers: { timestamp: event.at, txid: event.txid },
});

/** The issue rows and the State changes after one event. */
export function project(
  issues: IssueRows,
  event: WorkspaceEvent,
): readonly [IssueRows, ReadonlyArray<RowChange>] {
  return Match.valueTags(event, {
    ProjectCreated: (created) =>
      [issues, [DurableState.upsert("project", projectRow(created), headers(created))]] as const,
    IssueCreated: (created) => projectIssue(issues, created),
    IssueRenamed: (renamed) => projectIssue(issues, renamed),
    IssueStatusChanged: (changed) => projectIssue(issues, changed),
    CommentAdded: (added) =>
      [issues, [DurableState.upsert("comment", commentRow(added), headers(added))]] as const,
  });
}

function projectIssue(
  issues: IssueRows,
  event: IssueEvent,
): readonly [IssueRows, ReadonlyArray<RowChange>] {
  const row = issueAfter(issues[event.issueId], event);
  if (row === undefined) return [issues, []];
  return [{ ...issues, [row.id]: row }, [DurableState.upsert("issue", row, headers(event))]];
}

/**
 * The read model of each workspace. It folds the event stream into the base
 * app's rows and appends them to `Workspaces`, the State stream that the
 * browser reads. `Commit.producer` pins each unit and appends with a producer
 * sequence, so a retry after a crash does not write a row twice.
 */
export const readModel = Projection.mapAccum({
  id: "issue-tracker-rows",
  commit: Commit.producer,
  params: { workspaceId: Schema.String },
  inputs: { events: WorkspaceEvents },
  outputs: { workspace: Workspaces },
  state: { schema: IssueRows, initial: {} },
  mapAccum: (issues, { item }) => {
    const [next, changes] = project(issues, item);
    return [next, { workspace: changes }] as const;
  },
});
