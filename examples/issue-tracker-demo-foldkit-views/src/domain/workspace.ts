import { Array, Option, Order, Record, Schema, Struct } from "effect";
import { defineTaggedUnion } from "foldkit/schema";
import {
  Comment,
  Issue,
  IssueStatus,
  issueStatuses,
  Project,
} from "@streamsy/issue-tracker-demo-contract/state-schema";

// The workspace as plain data. Every function here is pure, so `update` and
// `view` call them directly and the tests call them with literals.

/** The rows that the State stream holds, by key. */
export const Rows = Schema.Struct({
  projects: Schema.Record(Schema.String, Project),
  issues: Schema.Record(Schema.String, Issue),
  comments: Schema.Record(Schema.String, Comment),
});

export type Rows = typeof Rows.Type;

export const emptyRows: Rows = { projects: {}, issues: {}, comments: {} };

/** The fields of an issue a user can edit. */
export const IssuePatch = Schema.Struct({
  title: Schema.optionalKey(Schema.String),
  status: Schema.optionalKey(IssueStatus),
});

export type IssuePatch = typeof IssuePatch.Type;

/**
 * One local change that the server has not confirmed. The row values carry
 * local times, so they sort at once. The server stamps its own times, and the
 * stream row replaces the local one when the txid arrives.
 */
export const Write = defineTaggedUnion({
  CreateProject: { project: Project },
  CreateIssue: { issue: Issue },
  UpdateIssue: {
    issueId: Schema.String,
    patch: IssuePatch,
    updatedAt: Schema.String,
    updatedBy: Schema.optionalKey(Schema.String),
  },
  CreateComment: { comment: Comment },
});

export type Write = typeof Write.Type;

export const PendingWrite = Schema.Struct({
  txid: Schema.String,
  write: Write,
  awaitOffset: Schema.optionalKey(Schema.String),
});

export type PendingWrite = typeof PendingWrite.Type;

/** A row as the page shows it, with whether a local change on it is pending. */
export interface Shown<A> {
  readonly row: A;
  readonly isPending: boolean;
}

/** What the page shows: the stream rows with every pending write on top. */
export interface Workspace {
  readonly projects: ReadonlyArray<Shown<Project>>;
  readonly issues: ReadonlyArray<Shown<Issue>>;
  readonly comments: ReadonlyArray<Shown<Comment>>;
}

/**
 * Lay the pending writes over the stream rows, oldest first. A failed write
 * leaves the pending list, so its change goes away on the next render. That is
 * the whole rollback.
 */
export const workspaceView = (rows: Rows, pending: ReadonlyArray<PendingWrite>): Workspace => {
  const overlaid = Array.reduce(pending, rows, (current, { write }) =>
    Write.match(write, {
      CreateProject: ({ project }) => ({
        ...current,
        projects: Record.set(current.projects, project.id, project),
      }),
      CreateIssue: ({ issue }) => ({
        ...current,
        issues: Record.set(current.issues, issue.id, issue),
      }),
      UpdateIssue: (update) => ({
        ...current,
        issues: Option.match(Record.get(current.issues, update.issueId), {
          onNone: () => current.issues,
          onSome: (issue) => Record.set(current.issues, issue.id, patchIssue(issue, update)),
        }),
      }),
      CreateComment: ({ comment }) => ({
        ...current,
        comments: Record.set(current.comments, comment.id, comment),
      }),
    }),
  );

  const pendingKeys = new Set(Array.map(pending, ({ write }) => writeKey(write)));

  const shown = <A extends { readonly id: string }>(row: A): Shown<A> => ({
    row,
    isPending: pendingKeys.has(row.id),
  });

  return {
    projects: Array.map(Record.values(overlaid.projects), shown),
    issues: Array.map(Record.values(overlaid.issues), shown),
    comments: Array.map(Record.values(overlaid.comments), shown),
  };
};

/** The issue after a pending edit. A writer with no name clears `updatedBy`, as on the server. */
const patchIssue = (issue: Issue, update: typeof Write.UpdateIssue.Type): Issue => {
  const patched = Struct.omit({ ...issue, ...update.patch, updatedAt: update.updatedAt }, [
    "updatedBy",
  ]);

  return update.updatedBy === undefined ? patched : { ...patched, updatedBy: update.updatedBy };
};

/** The id of the row that a write changes. */
const writeKey = (write: Write): string =>
  Write.match(write, {
    CreateProject: ({ project }) => project.id,
    CreateIssue: ({ issue }) => issue.id,
    UpdateIssue: ({ issueId }) => issueId,
    CreateComment: ({ comment }) => comment.id,
  });

/** The status filter: one status, or every issue. */
export const StatusFilter = Schema.Literals(["all", ...issueStatuses]);

export type StatusFilter = typeof StatusFilter.Type;

const byCreated = Order.mapInput(Order.String, (project: Shown<Project>) => project.row.createdAt);

const byName = Order.mapInput(Order.String, (project: Shown<Project>) => project.row.name);

/** All projects, oldest first. Projects with the same time sort by name. */
export const projectsOldestFirst = (workspace: Workspace) =>
  Array.sort(workspace.projects, Order.combine(byCreated, byName));

/** The selected project, or the first project when nothing valid is selected. */
export const selectedProject = (
  projects: ReadonlyArray<Shown<Project>>,
  maybeProjectId: Option.Option<string>,
): Option.Option<Shown<Project>> =>
  maybeProjectId.pipe(
    Option.flatMap((projectId) => Array.findFirst(projects, ({ row }) => row.id === projectId)),
    Option.orElse(() => Array.head(projects)),
  );
