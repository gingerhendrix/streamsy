/**
 * In-process active-expiry scheduler.
 *
 * Best-effort: meaningless in a short-lived serverless writer that exits before
 * the deadline, but a latency win for a long-lived HTTP host. Core remains the
 * source of truth via the durable `lifecycle.expiresAtMs` deadline enforced
 * lazily on reads, so a missed timer only defers reclamation, never corrupts.
 */
export class TimeoutScheduler {
  private timer?: ReturnType<typeof setTimeout>;

  constructor(private readonly callback?: () => Promise<void> | void) {}

  schedule(at: number): void {
    this.cancel();
    if (!this.callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timer = setTimeout(() => {
      void Promise.resolve(this.callback?.()).catch((error) => {
        console.error("storage-fs expiry callback failed", error);
      });
    }, delay);
  }

  cancel(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }
}
