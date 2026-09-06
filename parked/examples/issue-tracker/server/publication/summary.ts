/**
 * The `documentSink` runtime: one derived workspace summary.
 *
 * The document is built from the same maintained rows the board serves and the
 * same catalog ingestion the collections serve, so it cannot drift from them:
 * there is no second counter kept up to date by hand. Every board column is
 * present even when it is empty, so a consumer's rendering does not change
 * shape as a workspace fills up, and the counts are a deterministic function of
 * the committed rows — which is what makes the sink's entity tag stable.
 */
import { Effect } from "effect";
import type { CanonicalValue } from "@streamsy/sinks/fingerprint";
import { ISSUE_STATUSES, type IssueRow, type IssueStatus } from "../../domain/issue.ts";
import { PLAN_HASH } from "../config.ts";
import { catchUpStateSource, stateSourceId } from "../application/state-ingestion.ts";
import { advance } from "../application/maintenance.ts";
import { IssueStore } from "../persistence/store.ts";
import { ensureWorkspace } from "../transport/streams.ts";

/**
 * Build the current summary for one workspace.
 *
 * The maintained relation and the catalog are both brought to their durable
 * tails first, so the document is a summary of committed state rather than of
 * whatever happened to be in the store when the request arrived.
 */
export const buildWorkspaceSummary = Effect.fn("Summary.build")(function* (workspaceId: string) {
  const store = yield* IssueStore;
  yield* ensureWorkspace(workspaceId);
  yield* advance(workspaceId);
  const rows = yield* store.rows(workspaceId);

  const catalogCounts: Record<string, number> = {};
  for (const collection of ["projects", "users", "labels"] as const) {
    yield* catchUpStateSource(collection, workspaceId);
    const stored = yield* store.stateRows(stateSourceId(collection), collection, workspaceId);
    catalogCounts[collection] = stored.length;
  }

  return summaryOf(workspaceId, rows, {
    projects: catalogCounts.projects ?? 0,
    users: catalogCounts.users ?? 0,
    labels: catalogCounts.labels ?? 0,
  });
});

export interface CatalogCounts {
  readonly projects: number;
  readonly users: number;
  readonly labels: number;
}

/** The pure projection, so the document's shape is testable without a host. */
export function summaryOf(
  workspaceId: string,
  rows: readonly IssueRow[],
  catalogCounts: CatalogCounts,
): CanonicalValue {
  const byStatus = {
    backlog: 0,
    todo: 0,
    in_progress: 0,
    done: 0,
  } satisfies Record<IssueStatus, number>;
  let latestActivityAt: string | undefined;
  for (const row of rows) {
    byStatus[row.status] += 1;
    if (latestActivityAt === undefined || row.updatedAt > latestActivityAt) {
      latestActivityAt = row.updatedAt;
    }
  }
  return {
    workspaceId,
    planHash: PLAN_HASH,
    issues: {
      total: rows.length,
      byStatus: Object.fromEntries(ISSUE_STATUSES.map((status) => [status, byStatus[status]])),
    },
    catalog: {
      projects: catalogCounts.projects,
      users: catalogCounts.users,
      labels: catalogCounts.labels,
    },
    latestActivityAt: latestActivityAt ?? null,
  };
}
