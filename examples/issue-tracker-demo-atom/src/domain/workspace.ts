import { Array, Data, Match, Option, Order, Record, Struct } from "effect";
import type {
  Comment,
  Issue,
  IssueStatus,
  Project,
  StateEvent,
} from "@streamsy/issue-tracker-demo-contract/types";

// The workspace as plain data. Every function here is pure. The atoms in
// `src/queries.ts` call them, and the tests call them with literals.

/** The rows that the State stream holds, by key. */
export interface Rows {
  readonly projects: Record.ReadonlyRecord<string, Project>;
  readonly issues: Record.ReadonlyRecord<string, Issue>;
  readonly comments: Record.ReadonlyRecord<string, Comment>;
}

export const emptyRows: Rows = { projects: {}, issues: {}, comments: {} };

/** One read from the stream, or the end of the read. */
export type ReadEvent = Data.TaggedEnum<{
  Batch: { readonly changes: ReadonlyArray<StateEvent>; readonly isUpToDate: boolean };
  Failed: { readonly error: string };
}>;

export const ReadEvent = Data.taggedEnum<ReadEvent>();

/** What this browser has read from the stream so far. */
export interface Replica {
  readonly rows: Rows;
  /** Every txid that the stream has shown. A pending write leaves the overlay when its txid is here. */
  readonly confirmed: ReadonlySet<string>;
  /** The read has reached the head of the stream at least once. */
  readonly isLive: boolean;
  readonly maybeError: Option.Option<string>;
}

export const emptyReplica: Replica = {
  rows: emptyRows,
  confirmed: new Set(),
  isLive: false,
  maybeError: Option.none(),
};

const put = <A>(
  rows: Record.ReadonlyRecord<string, A>,
  key: string,
  operation: "delete" | "upsert",
  value: A | undefined,
) => {
  if (operation === "delete") return Record.remove(rows, key);

  return value === undefined ? rows : Record.set(rows, key, value);
};

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

/** Apply a batch of changes in stream order. */
export const applyChanges = (rows: Rows, changes: ReadonlyArray<StateEvent>): Rows =>
  Array.reduce(changes, rows, applyChange);

/** The txids that a batch of changes carries. */
export const txidsIn = (changes: ReadonlyArray<StateEvent>): ReadonlyArray<string> =>
  Array.dedupe(
    Array.getSomes(Array.map(changes, (change) => Option.fromUndefinedOr(change.headers.txid))),
  );

/** Fold one read event into the replica. */
export const applyReadEvent = (replica: Replica, event: ReadEvent): Replica =>
  ReadEvent.$match(event, {
    Batch: ({ changes, isUpToDate }) => ({
      ...replica,
      rows: applyChanges(replica.rows, changes),
      confirmed: new Set([...replica.confirmed, ...txidsIn(changes)]),
      isLive: replica.isLive || isUpToDate,
    }),
    Failed: ({ error }) => ({ ...replica, maybeError: Option.some(error) }),
  });

/** The fields of an issue a user can edit. */
export interface IssuePatch {
  readonly title?: string;
  readonly status?: IssueStatus;
}

/**
 * One local change that the server has not confirmed. The row values carry
 * local times, so they sort at once. The server stamps its own times, and the
 * stream row replaces the local one when the txid arrives.
 */
export type Write = Data.TaggedEnum<{
  CreateProject: { readonly project: Project };
  CreateIssue: { readonly issue: Issue };
  UpdateIssue: {
    readonly issueId: string;
    readonly patch: IssuePatch;
    readonly updatedAt: string;
    readonly updatedBy: Option.Option<string>;
  };
  CreateComment: { readonly comment: Comment };
}>;

export const Write = Data.taggedEnum<Write>();

export interface PendingWrite {
  readonly txid: string;
  readonly write: Write;
}

/** The writes whose txid the stream has not shown. */
export const unconfirmed = (
  writes: ReadonlyArray<PendingWrite>,
  confirmed: ReadonlySet<string>,
): ReadonlyArray<PendingWrite> => Array.filter(writes, ({ txid }) => !confirmed.has(txid));

/** A row as the page shows it. `$synced` is false while a local change on it is pending. */
export type Shown<A> = A & { readonly $synced: boolean };

/** What the page shows: the stream rows with every pending write on top. */
export interface Workspace {
  readonly projects: ReadonlyArray<Shown<Project>>;
  readonly issues: ReadonlyArray<Shown<Issue>>;
  readonly comments: ReadonlyArray<Shown<Comment>>;
}

/** The issue after a pending edit. A writer with no name clears `updatedBy`, as on the server. */
const patchIssue = (
  issue: Issue,
  { patch, updatedAt, updatedBy }: Data.TaggedEnum.Value<Write, "UpdateIssue">,
): Issue => {
  const patched = Struct.omit({ ...issue, ...patch, updatedAt }, ["updatedBy"]);

  return Option.match(updatedBy, {
    onNone: () => patched,
    onSome: (name) => ({ ...patched, updatedBy: name }),
  });
};

/** The id of the row that a write changes. */
const writeKey = (write: Write): string =>
  Write.$match(write, {
    CreateProject: ({ project }) => project.id,
    CreateIssue: ({ issue }) => issue.id,
    UpdateIssue: ({ issueId }) => issueId,
    CreateComment: ({ comment }) => comment.id,
  });

/**
 * Lay the pending writes over the stream rows, oldest first. A refused write
 * leaves the pending list, so its change goes away on the next read of this
 * value. That is the whole rollback.
 */
export const workspaceView = (rows: Rows, pending: ReadonlyArray<PendingWrite>): Workspace => {
  const overlaid = Array.reduce(pending, rows, (current, { write }) =>
    Write.$match(write, {
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
    ...row,
    $synced: !pendingKeys.has(row.id),
  });

  return {
    projects: Array.map(Record.values(overlaid.projects), shown),
    issues: Array.map(Record.values(overlaid.issues), shown),
    comments: Array.map(Record.values(overlaid.comments), shown),
  };
};

/** The status filter: one status, or every issue. */
export type StatusFilter = IssueStatus | "all";

const byCreated = Order.mapInput(Order.String, (project: Project) => project.createdAt);

const byName = Order.mapInput(Order.String, (project: Project) => project.name);

/** All projects, oldest first. Projects with the same time sort by name. */
export const projectsOldestFirst = (workspace: Workspace) =>
  Array.sort(workspace.projects, Order.combine(byCreated, byName));

/** The selected project, or the first project when nothing valid is selected. */
export const selectedProject = <A extends Project>(
  projects: ReadonlyArray<A>,
  maybeProjectId: Option.Option<string>,
): Option.Option<A> =>
  maybeProjectId.pipe(
    Option.flatMap((projectId) => Array.findFirst(projects, ({ id }) => id === projectId)),
    Option.orElse(() => Array.head(projects)),
  );

/** The issues of one project. */
export const issuesIn = (workspace: Workspace, projectId: string) =>
  Array.filter(workspace.issues, (issue) => issue.projectId === projectId);

/** How many rows have each key. */
const countBy = <A>(rows: ReadonlyArray<A>, key: (row: A) => string) =>
  Record.map(Array.groupBy(rows, key), (group) => group.length);

/** How many issues have each status, and how many there are. */
export const statusCounts = (issues: ReadonlyArray<Issue>): Record<StatusFilter, number> => {
  const counts = countBy(issues, (issue) => issue.status);

  return {
    all: issues.length,
    open: counts["open"] ?? 0,
    in_progress: counts["in_progress"] ?? 0,
    done: counts["done"] ?? 0,
  };
};

const byLastUpdate = Order.flip(Order.mapInput(Order.String, (issue: Issue) => issue.updatedAt));

/** The issues that pass the filter, last updated first. */
export const visibleIssues = <A extends Issue>(
  issues: ReadonlyArray<A>,
  filter: StatusFilter,
): ReadonlyArray<A> =>
  Array.sort(
    filter === "all" ? issues : Array.filter(issues, (issue) => issue.status === filter),
    byLastUpdate,
  );

/** How many issues each project has, by project id. */
export const issueCountByProject = (workspace: Workspace) =>
  new Map(Record.toEntries(countBy(workspace.issues, (issue) => issue.projectId)));

const byCommentTime = Order.mapInput(Order.String, (comment: Comment) => comment.createdAt);

/** Comments grouped by issue, oldest first in each group. */
export const commentsByIssue = (workspace: Workspace) =>
  new Map(
    Record.toEntries(
      Record.map(
        Array.groupBy(workspace.comments, (comment) => comment.issueId),
        (group) => Array.sort(group, byCommentTime),
      ),
    ),
  );
