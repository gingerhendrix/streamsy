/**
 * Browser tail of a durable State stream.
 *
 * The board and project rails are read from the Durable Streams HTTP endpoint:
 * one catch-up read from the beginning, then long-poll live reads from the
 * returned offset. A lost connection resumes from the last durable offset, so
 * convergence never depends on a command response.
 */
export type FeedStatus = "connecting" | "live" | "missing" | "reconnecting";

export interface FeedHandlers {
  readonly onItems: (items: readonly unknown[]) => void;
  readonly onStatus: (status: FeedStatus) => void;
  /** Called once the initial catch-up read has been applied. */
  readonly onReady?: () => void;
}

const MISSING_RETRY_MS = 1_500;
const MAX_BACKOFF_MS = 8_000;

/**
 * A long poll in flight when the page navigates away rejects with a generic
 * network error. That is expected teardown, not a failing stream, so it must
 * not be logged like one — while a real mid-session failure still must be.
 */
let navigating = false;
if (typeof globalThis.addEventListener === "function") {
  for (const event of ["pagehide", "beforeunload"] as const) {
    globalThis.addEventListener(event, () => {
      navigating = true;
    });
  }
}

export interface TeardownState {
  readonly aborted: boolean;
  readonly navigating: boolean;
}

/** True when a failed read is expected teardown rather than a retryable fault. */
export function isExpectedTeardown(error: unknown, state: TeardownState): boolean {
  if (state.aborted || state.navigating) return true;
  return error instanceof Error && error.name === "AbortError";
}

export function streamUrl(streamName: string, query: Record<string, string>): string {
  const path = streamName
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  const url = new URL(`/streams/${path}`, globalThis.location?.origin ?? "http://localhost");
  for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
  return url.toString();
}

/** Start tailing `streamName`. The returned function stops the tail. */
export function subscribeToStream(
  streamName: string,
  handlers: FeedHandlers,
  signal: AbortSignal,
): void {
  void run(streamName, handlers, signal);
}

async function run(streamName: string, handlers: FeedHandlers, signal: AbortSignal): Promise<void> {
  let offset = "-1";
  let cursor: string | undefined;
  let ready = false;
  let backoff = 500;

  while (!signal.aborted) {
    const live = offset !== "-1";
    if (!live) handlers.onStatus(ready ? "reconnecting" : "connecting");
    try {
      const response = await fetch(
        streamUrl(streamName, {
          offset,
          ...(live ? { live: "long-poll" } : {}),
          ...(live && cursor !== undefined ? { cursor } : {}),
        }),
        { signal, cache: "no-store" },
      );

      if (response.status === 404 || response.status === 410) {
        handlers.onStatus("missing");
        offset = "-1";
        cursor = undefined;
        await delay(MISSING_RETRY_MS, signal);
        continue;
      }
      if (!response.ok && response.status !== 204) {
        throw new Error(`stream read failed with ${response.status}`);
      }

      const next = response.headers.get("stream-next-offset");
      cursor = response.headers.get("stream-cursor") ?? undefined;
      if (response.status !== 204) {
        const items: unknown = await response.json();
        if (Array.isArray(items) && items.length > 0) handlers.onItems(items);
      }
      if (next !== null) offset = next;
      backoff = 500;
      handlers.onStatus("live");
      if (!ready) {
        ready = true;
        handlers.onReady?.();
      }
    } catch (error) {
      if (isExpectedTeardown(error, { aborted: signal.aborted, navigating })) return;
      console.warn(`stream tail ${streamName} retrying`, error);
      handlers.onStatus("reconnecting");
      await delay(backoff, signal);
      backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }
  }
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
    function finish() {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    }
  });
}
