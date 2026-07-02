/**
 * DO-local wake bus for the level-triggered `awaitChange` seam.
 *
 * A parked waiter resolves when a successful write calls {@link wake} or after a
 * capped timeout; the caller re-reads durable state to decide whether anything
 * relevant changed. The per-wait cap (`Math.min(timeoutMs, longPollTimeoutMs)`)
 * is defense-in-depth against an over-long single park. The binding bound on how
 * long an `awaitChange` RPC blocks the Durable Object is `runAwaitChangeLoop`,
 * which caps the loop's TOTAL budget against this same `longPollTimeoutMs`.
 */
export class DurableObjectNotifier {
  private readonly waiters = new Set<() => void>();

  /**
   * The DO long-poll cap (ms). Public so the `awaitChange` loop can bound its
   * total wait budget against the same value the per-park timeout uses — one
   * source of truth shared between the notifier and `storage.ts`.
   */
  constructor(readonly longPollTimeoutMs = 1500) {}

  waitForWake(timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      const finish = () => {
        clearTimeout(timeout);
        this.waiters.delete(finish);
        resolve();
      };
      const timeout = setTimeout(finish, Math.min(timeoutMs, this.longPollTimeoutMs));
      this.waiters.add(finish);
    });
  }

  /** Wake every parked `awaitChange` waiter. Called from inside a successful write. */
  wake(): void {
    if (this.waiters.size === 0) return;
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) waiter();
  }
}
