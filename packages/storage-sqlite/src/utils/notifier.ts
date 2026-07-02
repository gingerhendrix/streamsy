/**
 * In-process wake bus for the level-triggered `awaitChange` seam.
 *
 * Wakes same-process waiters on {@link wake}; otherwise a parked waiter resolves
 * after its timeout and the caller re-reads durable state. Writers in other
 * processes against a shared database file cannot wake these waiters, so live
 * reads are process-local notification plus a timeout fallback — a cross-process
 * write is observed on the next timeout re-read, not instantly. Full
 * cross-process pub/sub is out of scope for this adapter.
 */
export class StreamNotifier {
  private readonly waiters = new Set<() => void>();

  waitForWake(timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      const finish = () => {
        clearTimeout(timeout);
        this.waiters.delete(finish);
        resolve();
      };
      const timeout = setTimeout(finish, timeoutMs);
      this.waiters.add(finish);
    });
  }

  /** Wake every parked `awaitChange` waiter. Called from inside a mutation. */
  wake(): void {
    if (this.waiters.size === 0) return;
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) waiter();
  }
}
