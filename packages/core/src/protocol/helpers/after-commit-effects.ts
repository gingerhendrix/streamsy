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
    const notify = Promise.resolve(storage.notify(effects.notify));
    void notify.catch((error) => {
      console.error("stream notify after-commit effect failed", error);
    });
  }
}
