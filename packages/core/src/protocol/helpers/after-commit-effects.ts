import type { AfterCommitEffects, Stream } from "../../types/factory.ts";

/**
 * Runtime effects that are safe to fire only after the durable mutation has
 * committed. Storage adapters persist the MutationPlan atomically; core owns
 * scheduling and notification side effects so failed preconditions never fire
 * timers or wake readers.
 */
export async function runAfterCommit(
  effects: AfterCommitEffects | undefined,
  storage: Stream,
): Promise<void> {
  if (!effects) return;
  if (effects.cancelExpiry) await storage.cancelExpiry();
  else if (effects.scheduleExpiryAt !== undefined)
    await storage.scheduleExpiry(effects.scheduleExpiryAt);
  if (effects.notify) {
    // Await the notification so storage backends whose `notify` is a remote
    // call (e.g. the Durable Object stub RPC) reliably wake waiting readers
    // before the mutating request completes. On Cloudflare, un-awaited work can
    // be cancelled once the response returns, which would drop the wake of a
    // live reader sitting at the tail when a final message + close commit
    // together. A failed notification must not fail the committed mutation.
    try {
      await storage.notify(effects.notify);
    } catch (error) {
      console.error("stream notify after-commit effect failed", error);
    }
  }
}
