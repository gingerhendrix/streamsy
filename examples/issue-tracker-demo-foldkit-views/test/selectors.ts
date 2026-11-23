import { Array, Match, Order, Record } from "effect";
import type { Comment, Issue, StateEvent } from "@streamsy/issue-tracker-demo-contract/types";
import type { Rows, Shown, StatusFilter, Workspace } from "../src/domain/workspace.ts";

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
