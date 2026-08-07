/**
 * Optimistic mutation bookkeeping.
 *
 * An optimistic patch is a *display* overlay only. It is dropped as soon as the
 * durable board row read from the board State stream carries the same value, or
 * after a bounded hold if the projection has not landed yet. Nothing here is
 * ever treated as accepted state.
 */
import type { CoverageReport } from "../../shared/api.ts";
import type { BoardRow, IssuePriority, IssueStatus } from "../../shared/model.ts";

export type MutationPhase = "syncing" | "synced" | "failed";

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
  readonly ack?: { readonly stream: string; readonly position: string };
  readonly coverage?: CoverageReport;
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
    if (mutation.phase === "syncing" || mutation.phase === "failed") return true;
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
    merged.set(mutation.issueId, {
      ...existing,
      ...(mutation.patch.title === undefined ? {} : { title: mutation.patch.title }),
      ...(mutation.patch.status === undefined ? {} : { status: mutation.patch.status }),
      ...(mutation.patch.priority === undefined ? {} : { priority: mutation.patch.priority }),
      ...(mutation.patch.assigneeId === undefined ? {} : { assigneeId: mutation.patch.assigneeId }),
      ...(mutation.patch.commentCount === undefined
        ? {}
        : { commentCount: Math.max(existing.commentCount, mutation.patch.commentCount) }),
    });
  }
  return Array.from(merged.values());
}

export type CardSync = "idle" | "syncing" | "failed" | "synced";

/** The strongest sync state to show on one card. Failure wins over progress. */
export function cardSync(issueId: string, overlays: readonly Mutation[]): CardSync {
  let state: CardSync = "idle";
  for (const mutation of overlays) {
    if (mutation.issueId !== issueId) continue;
    if (mutation.phase === "failed") return "failed";
    state = mutation.phase === "syncing" ? "syncing" : state === "idle" ? "synced" : state;
  }
  return state;
}

/** Workspace-level status line for the header and the live region. */
export function syncSummary(overlays: readonly Mutation[]): {
  readonly state: CardSync;
  readonly count: number;
} {
  const failed = overlays.filter((mutation) => mutation.phase === "failed").length;
  if (failed > 0) return { state: "failed", count: failed };
  const syncing = overlays.filter((mutation) => mutation.phase === "syncing").length;
  if (syncing > 0) return { state: "syncing", count: syncing };
  return { state: overlays.length > 0 ? "synced" : "idle", count: overlays.length };
}
