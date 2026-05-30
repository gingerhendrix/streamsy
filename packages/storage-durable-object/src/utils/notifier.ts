import type { StreamEventType, WaitForEventOptions, WaitForEventResult } from "@streamsy/core";

export class DurableObjectNotifier {
  private readonly waiters = new Set<(result: WaitForEventResult) => void>();

  constructor(private readonly longPollTimeoutMs = 1500) {}

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return new Promise((resolve) => {
      const timeout = setTimeout(
        () => finish({ status: "timeout" }),
        Math.min(options.timeoutMs, this.longPollTimeoutMs),
      );
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
