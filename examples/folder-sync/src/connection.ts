/**
 * Whether the stream reader can reach the server. The stream client retries a
 * failed read with backoff and never gives up, so the page only needs to know
 * whether the last read attempt got an answer.
 */
export interface Connection {
  readonly fetch: typeof globalThis.fetch;
  readonly isOnline: () => boolean;
  readonly subscribe: (listener: () => void) => () => void;
}

export function trackConnection(): Connection {
  let online = true;
  const listeners = new Set<() => void>();

  const set = (next: boolean) => {
    if (next === online) return;
    online = next;

    for (const listener of listeners) listener();
  };

  // Any HTTP answer, even an error status, means the server is reachable. A
  // rejected fetch is a network failure, unless the reader aborted it on close.
  const tracked = (input: RequestInfo | URL, init?: RequestInit) => {
    // oxlint-disable-next-line effecttsgo/global-fetch -- The stream client requires native Response and AbortSignal behavior from its fetch adapter.
    return globalThis.fetch(input, init).then(
      (response) => {
        set(true);

        return response;
      },
      (error) => {
        if (!init?.signal?.aborted) set(false);
        throw error;
      },
    );
  };

  return {
    fetch: Object.assign(tracked, { preconnect: globalThis.fetch.preconnect }),
    isOnline: () => online,
    subscribe: (listener) => {
      listeners.add(listener);

      return () => listeners.delete(listener);
    },
  };
}
