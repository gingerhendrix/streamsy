/**
 * Optimistic mutation bookkeeping.
 *
 * An optimistic patch is a *display* overlay only. It is dropped as soon as the
 * durable board row read from the board State stream carries the same value, or
 * after a bounded hold if the projection has not landed yet. Nothing here is
 * ever treated as accepted state.
 */
import type { CoverageReport, ProjectionPassReport } from "../../shared/api.ts";
import type { BoardRow, IssuePriority, IssueStatus } from "../../shared/model.ts";

/**
 * `pending` is the accepted-but-unproven phase: the command is durable, and the
 * project board does not cover it yet. It exists so `synced` can keep its one
 * meaning.
 */
export type MutationPhase = "syncing" | "pending" | "synced" | "failed";

export interface PendingPatch {
  readonly title?: string;
  readonly status?: IssueStatus;
  readonly priority?: IssuePriority;
  readonly assigneeId?: string | null;
  /** Absolute expected comment count, not a delta. */
  readonly commentCount?: number;
}

export interface Mutation {
  readonly commandId: string;
  readonly issueId: string;
  readonly projectId: string;
  /** Human label for the live region and the inspector. */
  readonly label: string;
  readonly phase: MutationPhase;
  readonly patch: PendingPatch;
  /** Placeholder card for an issue that has no durable row yet. */
  readonly insert?: BoardRow;
  readonly startedAt: number;
  readonly settledAt?: number;
  readonly error?: string;
  /** Non-failing explanation for a pending mutation, shown beside the chip. */
  readonly note?: string;
  readonly ack?: { readonly stream: string; readonly position: string };
  readonly coverage?: CoverageReport;
  readonly projections?: readonly ProjectionPassReport[];
}

/** What the server reported about one accepted command. */
export interface SettlementInput {
  readonly coverage: CoverageReport;
  readonly projections: readonly ProjectionPassReport[];
}

export interface Settlement {
  readonly phase: "pending" | "synced" | "failed";
  readonly message: string;
}

/**
 * The core product law.
 *
 * `Synced` means the accepted source acknowledgement is durably covered by the
 * project board: proven chained coverage, and no projection pass that reported
 * a fault. A conflict, poison, unknown member, oversized boundary, or exhausted
 * limit is never `Synced`; `not-yet` and `incomparable` stay pending until a
 * later lineage probe proves them.
 */
export function classifySettlement(label: string, result: SettlementInput): Settlement {
  const faulted = result.projections.find((pass) => pass.outcome === "faulted");
  if (faulted !== undefined) {
    const because = faulted.detail === undefined ? "" : ` — ${faulted.detail}`;
    return {
      phase: "failed",
      message: `${label} failed: the ${faulted.label} projection reported ${faulted.status}${because}.`,
    };
  }
  if (result.coverage.status === "proven") {
    return { phase: "synced", message: `${label}: synced and proven through both projections.` };
  }
  return {
    phase: "pending",
    message: `${label}: accepted at ${result.coverage.ack.position}; the ${
      result.coverage.blockedAt ?? "next"
    } hop does not cover it yet (${result.coverage.status}).`,
  };
}

/** How long a settled overlay is held while waiting for the durable row. */
export const OVERLAY_HOLD_MS = 6_000;

/**
 * How long `Synced` stays legible after a command settles. Without it the
 * durable row usually arrives first and the result never reaches the screen.
 */
export const SETTLED_DISPLAY_MS = 2_500;

export function matchesPatch(row: BoardRow | undefined, patch: PendingPatch): boolean {
  if (row === undefined) return false;
  if (patch.title !== undefined && row.title !== patch.title) return false;
  if (patch.status !== undefined && row.status !== patch.status) return false;
  if (patch.priority !== undefined && row.priority !== patch.priority) return false;
  if (patch.assigneeId !== undefined && row.assigneeId !== patch.assigneeId) return false;
  if (patch.commentCount !== undefined && row.commentCount < patch.commentCount) return false;
  return true;
}

/** Mutations whose overlay should still be painted over the durable board. */
export function activeOverlays(
  mutations: readonly Mutation[],
  rows: ReadonlyMap<string, BoardRow>,
  now: number,
): readonly Mutation[] {
  return mutations.filter((mutation) => {
    // A pending mutation is unproven, so its overlay is never retired on a
    // timer: it stays until a probe proves or fails it.
    if (
      mutation.phase === "syncing" ||
      mutation.phase === "pending" ||
      mutation.phase === "failed"
    ) {
      return true;
    }
    const elapsed = now - (mutation.settledAt ?? mutation.startedAt);
    if (elapsed < SETTLED_DISPLAY_MS) return true;
    if (matchesPatch(rows.get(mutation.issueId), mutation.patch)) return false;
    return elapsed < OVERLAY_HOLD_MS;
  });
}

/** Durable rows with pending overlays applied, including optimistic inserts. */
export function overlayRows(
  rows: ReadonlyMap<string, BoardRow>,
  overlays: readonly Mutation[],
): readonly BoardRow[] {
  const merged = new Map(rows);
  for (const mutation of overlays) {
    const existing = merged.get(mutation.issueId) ?? mutation.insert;
    if (existing === undefined) continue;
    const patched: BoardRow = {
      ...existing,
      title: mutation.patch.title ?? existing.title,
      status: mutation.patch.status ?? existing.status,
      priority: mutation.patch.priority ?? existing.priority,
      assigneeId:
        mutation.patch.assigneeId === undefined ? existing.assigneeId : mutation.patch.assigneeId,
      commentCount:
        mutation.patch.commentCount === undefined
          ? existing.commentCount
          : Math.max(existing.commentCount, mutation.patch.commentCount),
    };
    merged.set(mutation.issueId, patched);
  }
  return Array.from(merged.values());
}

export type CardSync = "idle" | "synced" | "pending" | "syncing" | "failed";

/** Strongest state first: nothing weaker may hide a failure or an unproven hop. */
const SYNC_RANK = {
  idle: 0,
  synced: 1,
  pending: 2,
  syncing: 3,
  failed: 4,
} satisfies Readonly<Record<CardSync, number>>;

/** The strongest sync state to show on one card. Failure wins over progress. */
export function cardSync(issueId: string, overlays: readonly Mutation[]): CardSync {
  let state: CardSync = "idle";
  for (const mutation of overlays) {
    if (mutation.issueId !== issueId) continue;
    const candidate: CardSync = mutation.phase;
    if (SYNC_RANK[candidate] > SYNC_RANK[state]) state = candidate;
  }
  return state;
}

/** Workspace-level status line for the header and the live region. */
export interface SyncSummary {
  readonly state: CardSync;
  readonly count: number;
}

export function syncSummary(overlays: readonly Mutation[]): SyncSummary {
  for (const phase of ["failed", "syncing", "pending"] as const) {
    const count = overlays.filter((mutation) => mutation.phase === phase).length;
    if (count > 0) return { state: phase, count };
  }
  return { state: overlays.length > 0 ? "synced" : "idle", count: overlays.length };
}

export interface CardFailure {
  readonly commandId: string;
  readonly message: string;
}

/**
 * The newest failed mutation for one issue.
 *
 * Retry must replay the exact command that failed. `overlays` is newest-first,
 * so the first match is the newest failure; retrying an older command for the
 * same issue would leave the visible failure untouched.
 */
export function latestFailure(
  issueId: string,
  overlays: readonly Mutation[],
): CardFailure | undefined {
  const failed = overlays.find(
    (mutation) => mutation.issueId === issueId && mutation.phase === "failed",
  );
  return failed === undefined
    ? undefined
    : { commandId: failed.commandId, message: failed.error ?? "Sync failed" };
}
