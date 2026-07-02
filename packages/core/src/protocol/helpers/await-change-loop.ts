/**
 * The exported level-triggered `awaitChange` loop for adapter authors.
 *
 * Every faithful `awaitChange` implementation is the same loop: re-read durable
 * state, return `changed` if the snapshot differs from what the caller observed,
 * otherwise park until a wake or the remaining budget expires, then re-check.
 * Adapters supply only `readRecord` (may be async) and `waitForWake`; a minimal
 * polling adapter's `waitForWake` is a plain sleep.
 *
 * Two optional caps keep lossy or remote backends honest:
 *
 * - `totalCapMs` caps the TOTAL wait budget (e.g. a Durable Object long-poll
 *   cap, so a single RPC never strands the actor for the caller's full
 *   timeout). Returning `timeout` early is licensed by the contract — callers
 *   re-park.
 * - `parkCapMs` caps each individual park. If your wake source is lossy or your
 *   read→register window is not atomic w.r.t. wakes (any async-I/O backend), a
 *   per-park cap guarantees a missed wake is repaired within the cap — the loop
 *   is bounded-stale by construction.
 */
import type { AwaitChangeOptions, AwaitChangeResult, StreamRecord } from "../../types/storage.ts";
import { buildChangeSnapshot, changeSnapshotDiffers } from "./change-snapshot.ts";

export interface AwaitChangeLoopDeps {
  /** Read the current durable record. May be sync or async. */
  readRecord(): StreamRecord | null | Promise<StreamRecord | null>;
  /** Park until a wake or the given timeout, whichever comes first. */
  waitForWake(timeoutMs: number): Promise<void>;
  /** Cap the TOTAL wait budget (early `timeout` return; callers re-park). */
  totalCapMs?: number;
  /** Cap each individual park so a lost wake is repaired within the cap. */
  parkCapMs?: number;
  /** Wall clock; overridable for deterministic tests. */
  now?: () => number;
}

export async function runAwaitChangeLoop(
  deps: AwaitChangeLoopDeps,
  options: AwaitChangeOptions,
): Promise<AwaitChangeResult> {
  const now = deps.now ?? (() => Date.now());
  const budget =
    deps.totalCapMs === undefined
      ? options.timeoutMs
      : Math.min(options.timeoutMs, deps.totalCapMs);
  const start = now();

  while (true) {
    const snapshot = buildChangeSnapshot(await deps.readRecord());
    if (changeSnapshotDiffers(snapshot, options)) return { status: "changed", snapshot };

    const remaining = budget - (now() - start);
    if (remaining <= 0) return { status: "timeout", snapshot };
    const park = deps.parkCapMs === undefined ? remaining : Math.min(remaining, deps.parkCapMs);
    await deps.waitForWake(park);
  }
}
