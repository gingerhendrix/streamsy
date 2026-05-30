import type { StreamEventType, WaitForEventOptions, WaitForEventResult } from "@streamsy/core";

/**
 * In-process live-read notifier.
 *
 * Wakes same-process waiters on `notify`; otherwise waiters resolve to
 * `timeout`. Writers in other processes against a shared database file cannot
 * be observed here, so live reads are process-local notification plus a timeout
 * fallback — full cross-process pub/sub is out of scope for this adapter.
 */
export class StreamNotifier {
  private readonly waiters = new Set<(result: WaitForEventResult) => void>();

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return new Promise((resolve) => {
      const timeout = setTimeout(() => finish({ status: "timeout" }), options.timeoutMs);
      const finish = (result: WaitForEventResult) => {
        clearTimeout(timeout);
        this.waiters.delete(finish);
        resolve(result);
      };
      this.waiters.add(finish);
      options.signal?.addEventListener("abort", () => finish({ status: "aborted" }), {
        once: true,
      });
    });
  }

  notify(type: StreamEventType): void {
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) waiter({ status: "notified", type });
  }
}
