/**
 * In-process wake bus for the level-triggered `awaitChange` seam.
 *
 * Wakes same-process waiters immediately on {@link wake}. Cross-process writers
 * cannot reach these callbacks — that is what `fs.watch` (and the bounded poll
 * fallback) are for in `FsStream.awaitChange`. A woken waiter always re-reads
 * durable state and re-parks if nothing relevant changed, so over-waking is safe.
 */
export class Notifier {
  private readonly waiters = new Set<() => void>();

  /** Register a one-shot-style wake callback; returns an unregister function. */
  register(callback: () => void): () => void {
    this.waiters.add(callback);
    return () => {
      this.waiters.delete(callback);
    };
  }

  /** Wake every currently-parked waiter. */
  wake(): void {
    if (this.waiters.size === 0) return;
    const callbacks = [...this.waiters];
    this.waiters.clear();
    for (const callback of callbacks) callback();
  }
}
