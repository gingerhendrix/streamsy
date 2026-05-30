/**
 * In-process active-expiry scheduler.
 *
 * Good enough for a local Bun process; lazy `expireIfNeeded` in core remains the
 * source of truth. Across a process restart, active expiry is deferred until the
 * next access or an application-provided sweep.
 */
export class TimeoutScheduler {
  private timer?: ReturnType<typeof setTimeout>;

  schedule(at: number, callback?: () => Promise<void>): void {
    this.cancel();
    if (!callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timer = setTimeout(() => void callback(), delay);
  }

  cancel(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
  }
}
