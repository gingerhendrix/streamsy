import { count, eq, useLiveQuery } from "@tanstack/react-db";
import { Array } from "effect";
import { useMemo } from "react";
import type { Comment, IssueStatus } from "@streamsy/issue-tracker-demo-contract/types";
import type { IssueDb } from "./db.ts";

/** The status filter: one status, or every issue. */
export type StatusFilter = IssueStatus | "all";

// Every value the page derives from the workspace is a live query. TanStack DB
// keeps each result up to date as rows arrive from the stream or from an
// optimistic write, and a row keeps its `$synced` flag through the query.

/** All projects, oldest first. Projects with the same time sort by name. */
export function useProjects(db: IssueDb) {
  const { data } = useLiveQuery((q) =>
    q
      .from({ project: db.collections.projects })
      .orderBy(({ project }) => project.createdAt)
      .orderBy(({ project }) => project.name),
  );

  return data ?? [];
}

/** The issue count of each project, by project id. */
export function useIssueCountByProject(db: IssueDb) {
  const { data } = useLiveQuery((q) =>
    q
      .from({ issue: db.collections.issues })
      .groupBy(({ issue }) => issue.projectId)
      .select(({ issue }) => ({ projectId: issue.projectId, issues: count(issue.id) })),
  );

  return useMemo(
    () => new Map(Array.map(data ?? [], (row) => [row.projectId, row.issues] as const)),
    [data],
  );
}

/** The issues of one project that pass the filter, last updated first. */
export function useVisibleIssues(db: IssueDb, projectId: string, filter: StatusFilter) {
  const { data } = useLiveQuery(
    (q) => {
      const inProject = q
        .from({ issue: db.collections.issues })
        .where(({ issue }) => eq(issue.projectId, projectId));

      const filtered =
        filter === "all" ? inProject : inProject.where(({ issue }) => eq(issue.status, filter));

      return filtered.orderBy(({ issue }) => issue.updatedAt, "desc");
    },
    [projectId, filter],
  );

  return data ?? [];
}

/** How many issues of one project have each status. */
export function useStatusCounts(db: IssueDb, projectId: string) {
  const { data } = useLiveQuery(
    (q) =>
      q
        .from({ issue: db.collections.issues })
        .where(({ issue }) => eq(issue.projectId, projectId))
        .groupBy(({ issue }) => issue.status)
        .select(({ issue }) => ({ status: issue.status, issues: count(issue.id) })),
    [projectId],
  );

  return useMemo(() => {
    const counts = { all: 0, open: 0, in_progress: 0, done: 0 };

    for (const row of data ?? []) {
      counts[row.status] = row.issues;
      counts.all += row.issues;
    }

    return counts;
  }, [data]);
}

/** Comments grouped by issue, oldest first in each group. */
export function useCommentsByIssue(db: IssueDb) {
  const { data } = useLiveQuery((q) =>
    q.from({ comment: db.collections.comments }).orderBy(({ comment }) => comment.createdAt),
  );

  return useMemo(() => {
    const grouped = new Map<string, globalThis.Array<Comment & { readonly $synced: boolean }>>();

    for (const comment of data ?? []) {
      grouped.set(comment.issueId, [...(grouped.get(comment.issueId) ?? []), comment]);
    }

    return grouped;
  }, [data]);
}
