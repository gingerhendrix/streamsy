/**
 * In-process wake bus backing the memory adapter's level-triggered `awaitChange`.
 *
 * A parked waiter resolves either when a mutation calls {@link wake} or after its
 * timeout; the caller re-reads durable state to decide whether anything relevant
 * changed. Spurious wakes are safe — the caller simply re-checks and parks again
 * within its remaining budget.
 */
export class MemoryNotifier {
  private readonly wakeWaiters = new Set<() => void>();

  waitForWake(timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      const finish = () => {
        clearTimeout(timeout);
        this.wakeWaiters.delete(finish);
        resolve();
      };
      const timeout = setTimeout(finish, timeoutMs);
      this.wakeWaiters.add(finish);
    });
  }

  /** Wake every parked `awaitChange` waiter. Called from inside a mutation. */
  wake(): void {
    if (this.wakeWaiters.size === 0) return;
    const waiters = [...this.wakeWaiters];
    this.wakeWaiters.clear();
    for (const waiter of waiters) waiter();
  }
}
