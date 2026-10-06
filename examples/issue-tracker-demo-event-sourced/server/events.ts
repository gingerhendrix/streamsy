import {
  Comment,
  Issue,
  IssueStatus,
  Project,
} from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Match, Schema } from "effect";

/**
 * Fields on every event. The server stamps the time, so a replay gives the same
 * rows. The txid is the one the browser waits for on the State stream.
 */
const stamped = { at: Schema.String, txid: Schema.String };

/** The display name of the writer, when the writer gave one. */
const writer = { updatedBy: Schema.optionalKey(Schema.String) };

/**
 * The domain events of one workspace. Each event names what a user did. A
 * rename and a status change are separate events, where the base app writes one
 * issue upsert for both.
 */
export const WorkspaceEvent = Schema.TaggedUnion({
  ProjectCreated: {
    projectId: Schema.String,
    name: Schema.String,
    description: Schema.String,
    ...stamped,
  },
  IssueCreated: {
    issueId: Schema.String,
    projectId: Schema.String,
    title: Schema.String,
    status: IssueStatus,
    ...writer,
    ...stamped,
  },
  IssueRenamed: { issueId: Schema.String, title: Schema.String, ...writer, ...stamped },
  IssueStatusChanged: { issueId: Schema.String, status: IssueStatus, ...writer, ...stamped },
  CommentAdded: {
    commentId: Schema.String,
    issueId: Schema.String,
    author: Schema.String,
    body: Schema.String,
    ...stamped,
  },
});

export type WorkspaceEvent = typeof WorkspaceEvent.Type;

type Cases = typeof WorkspaceEvent.cases;

export type ProjectCreated = Cases["ProjectCreated"]["Type"];

export type IssueCreated = Cases["IssueCreated"]["Type"];

export type IssueRenamed = Cases["IssueRenamed"]["Type"];

export type IssueStatusChanged = Cases["IssueStatusChanged"]["Type"];

export type CommentAdded = Cases["CommentAdded"]["Type"];

/** The events that change an issue row. */
export type IssueEvent = IssueCreated | IssueRenamed | IssueStatusChanged;

export const projectRow = (event: ProjectCreated): Project => ({
  id: event.projectId,
  name: event.name,
  description: event.description,
  createdAt: event.at,
});

export const commentRow = (event: CommentAdded): Comment => ({
  id: event.commentId,
  issueId: event.issueId,
  author: event.author,
  body: event.body,
  createdAt: event.at,
});

/**
 * The issue row after one event. The command view and the read model both use
 * it, so a decision and the row in the browser agree. An edit of an issue that
 * the row set does not hold gives `undefined`. A decision never appends one.
 */
export function issueAfter(previous: Issue | undefined, event: IssueEvent): Issue | undefined {
  return Match.valueTags(event, {
    IssueCreated: (created) => ({
      id: created.issueId,
      projectId: created.projectId,
      title: created.title,
      status: created.status,
      createdAt: created.at,
      updatedAt: created.at,
      ...lastWriter(created.updatedBy),
    }),
    IssueRenamed: (renamed) => previous && { ...edited(previous, renamed), title: renamed.title },
    IssueStatusChanged: (changed) =>
      previous && { ...edited(previous, changed), status: changed.status },
  });
}

/** The row with this edit's time and writer. `updatedBy` names this writer, or nobody. */
function edited(previous: Issue, event: IssueEvent): Issue {
  const { updatedBy: _previousWriter, ...row } = previous;

  return { ...row, updatedAt: event.at, ...lastWriter(event.updatedBy) };
}

function lastWriter(name: string | undefined): Pick<Issue, "updatedBy"> {
  return name === undefined ? {} : { updatedBy: name };
}
