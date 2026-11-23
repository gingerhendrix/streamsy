import { Array, Match, Option, Order, Record, Schema, Struct } from "effect";
import { defineTaggedUnion } from "foldkit/schema";
import {
  Comment,
  Issue,
  IssueStatus,
  issueStatuses,
  Project,
  type StateEvent,
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

/** Apply one change from the stream. An upsert without a value changes nothing. */
const applyChange = (rows: Rows, change: StateEvent): Rows =>
  Match.value(change).pipe(
    Match.discriminatorsExhaustive("type")({
      project: ({ key, value, headers }) => ({
        ...rows,
        projects: put(rows.projects, key, headers.operation, value),
      }),
      issue: ({ key, value, headers }) => ({
        ...rows,
        issues: put(rows.issues, key, headers.operation, value),
      }),
      comment: ({ key, value, headers }) => ({
        ...rows,
        comments: put(rows.comments, key, headers.operation, value),
      }),
    }),
  );

const put = <A>(
  rows: Record.ReadonlyRecord<string, A>,
  key: string,
  operation: "delete" | "upsert",
  value: A | undefined,
) => {
  if (operation === "delete") return Record.remove(rows, key);

  return value === undefined ? rows : Record.set(rows, key, value);
};

/** Apply a batch of changes in stream order. */
export const applyChanges = (rows: Rows, changes: ReadonlyArray<StateEvent>): Rows =>
  Array.reduce(changes, rows, applyChange);

/** The txids that a batch of changes confirms. */
export const confirmedTxids = (changes: ReadonlyArray<StateEvent>): ReadonlyArray<string> =>
  Array.dedupe(
    Array.getSomes(Array.map(changes, (change) => Option.fromUndefinedOr(change.headers.txid))),
  );

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

export const PendingWrite = Schema.Struct({ txid: Schema.String, write: Write });

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

// Derived lists. The view calls these on every render, as the React base calls
// its live queries. A workspace is small, so nothing is cached.

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

/** The issues of one project. */
export const issuesIn = (workspace: Workspace, projectId: string) =>
  Array.filter(workspace.issues, ({ row }) => row.projectId === projectId);

/** How many issues have each status, and how many there are. */
export const statusCounts = (issues: ReadonlyArray<Shown<Issue>>): Record<StatusFilter, number> => {
  const counts = countBy(issues, ({ row }) => row.status);

  return {
    all: issues.length,
    open: counts.open ?? 0,
    in_progress: counts.in_progress ?? 0,
    done: counts.done ?? 0,
  };
};

const byLastUpdate = Order.flip(
  Order.mapInput(Order.String, (issue: Shown<Issue>) => issue.row.updatedAt),
);

/** The issues that pass the filter, last updated first. */
export const visibleIssues = (issues: ReadonlyArray<Shown<Issue>>, filter: StatusFilter) =>
  Array.sort(
    filter === "all" ? issues : Array.filter(issues, ({ row }) => row.status === filter),
    byLastUpdate,
  );

/** How many issues each project has, by project id. */
export const issueCountByProject = (workspace: Workspace) =>
  countBy(workspace.issues, ({ row }) => row.projectId);

/** How many rows have each key. */
const countBy = <A>(rows: ReadonlyArray<A>, key: (row: A) => string) =>
  Record.map(Array.groupBy(rows, key), (group) => group.length);

const byCommentTime = Order.mapInput(
  Order.String,
  (comment: Shown<Comment>) => comment.row.createdAt,
);

/** The comments of one issue, oldest first. */
export const commentsOn = (workspace: Workspace, issueId: string) =>
  Array.sort(
    Array.filter(workspace.comments, ({ row }) => row.issueId === issueId),
    byCommentTime,
  );
