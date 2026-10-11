/** Browser events wake a sleeping network retry without replacing the live collections. */
export interface RetryEvents {
  readonly online: EventTarget;
  readonly visibility: EventTarget;
  readonly isVisible: () => boolean;
}

export interface Connection {
  readonly fetch: typeof globalThis.fetch;
  readonly isOnline: () => boolean;
  readonly subscribe: (listener: () => void) => () => void;
}

export function trackConnection(
  fetcher: typeof globalThis.fetch = globalThis.fetch,
  events?: RetryEvents,
): Connection {
  let online = true;
  const listeners = new Set<() => void>();
  const set = (next: boolean) => {
    if (next === online) return;
    online = next;
    for (const listener of listeners) listener();
  };

  const wait = (delay: number, signal?: AbortSignal | null) =>
    new Promise<boolean>((resolve, reject) => {
      const finish = (prompted: boolean) => {
        cleanup();
        resolve(prompted);
      };
      const wake = () => finish(true);
      const visible = () => {
        if (events?.isVisible()) wake();
      };
      const abort = () => {
        cleanup();
        reject(signal?.reason);
      };
      const timer = setTimeout(() => finish(false), delay);
      const cleanup = () => {
        clearTimeout(timer);
        events?.online.removeEventListener("online", wake);
        events?.visibility.removeEventListener("visibilitychange", visible);
        signal?.removeEventListener("abort", abort);
      };
      events?.online.addEventListener("online", wake);
      events?.visibility.addEventListener("visibilitychange", visible);
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
    });

  const tracked = async (input: RequestInfo | URL, init?: RequestInit) => {
    const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
    let delay = 100;

    while (true) {
      try {
        // oxlint-disable-next-line effecttsgo/global-fetch -- The stream client requires native Response and AbortSignal behavior from its fetch adapter.
        const response = await fetcher(input, init);
        set(true);
        return response;
      } catch (error) {
        if (signal?.aborted) throw error;
        set(false);
        // Retain this read and its cursor. Close aborts both fetch and retry waits.
        const prompted = await wait(delay, signal);
        delay = prompted ? 100 : Math.min(delay * 2, 30_000);
      }
    }
  };

  return {
    fetch: Object.assign(tracked, { preconnect: fetcher.preconnect }),
    isOnline: () => online,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}
