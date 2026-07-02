import type { BoundStream } from "./bind-stream.ts";

/**
 * Core-side after-commit effects. These live on core's plan *decisions*, NOT on
 * the seam plan types: core executes them itself (calling `scheduleExpiry` /
 * `cancelExpiry` back on the adapter) after the adapter's mutation returns.
 * Adapters never see or run them — putting them on the plans taught a
 * double-execution bug.
 */
export interface AfterCommitEffects {
  scheduleExpiryAt?: number;
  cancelExpiry?: boolean;
}

/**
 * Runtime effects that are safe to fire only after the durable mutation has
 * committed. Storage adapters persist each intent atomically; core owns expiry
 * scheduling so failed preconditions never fire timers.
 *
 * Waking live readers is not an after-commit effect: the level-triggered
 * `awaitChange` seam re-reads durable state, so an adapter wakes its own waiters
 * from inside a successful mutation (a pure latency optimization). Correctness
 * never depends on a notification crossing the seam.
 */
export async function runAfterCommit(
  effects: AfterCommitEffects | undefined,
  storage: BoundStream,
): Promise<void> {
  if (!effects) return;
  if (effects.cancelExpiry) await storage.cancelExpiry();
  else if (effects.scheduleExpiryAt !== undefined)
    await storage.scheduleExpiry(effects.scheduleExpiryAt);
}
