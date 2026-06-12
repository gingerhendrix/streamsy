import type {
  StreamEventType,
  WaitForEventOptions,
  WaitForEventResult,
} from "../../../types/storage.ts";

export class MemoryNotifier {
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
