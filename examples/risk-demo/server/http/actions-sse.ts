/* oxlint-disable effecttsgo/async-function -- Web-standard fetch handlers are Promise-native framework adapters; they delegate game and projection work to the existing application services and runtime. */
/* oxlint-disable effecttsgo/global-console, effecttsgo/global-date, effecttsgo/global-timers -- The Web ReadableStream adapter owns connection deadlines, elapsed-time accounting, and best-effort terminal logging outside reusable Effect orchestration. */
/**
 * The server half of the actions stream: a bounded `text/event-stream` response
 * over a player's durable action-required stream.
 *
 * Shape of one connection:
 *   1. the backlog from `?offset=` is written immediately, so a reconnecting
 *      client never waits for the next thing to happen to learn what it missed;
 *   2. the connection then blocks on the derived stream until an action lands,
 *      writing a `data`/`control` pair for each;
 *   3. after `timeoutMs` of wall time the server closes, and the client
 *      reconnects from the last `nextOffset` it saw.
 *
 * Every batch ends with a `control` event, including the empty ones a blocked
 * read produces when it expires — so the cursor is always re-stated, and a
 * client's resume position is never inferred from silence.
 *
 * The wall bound uses real time deliberately: the injected clock is frozen in
 * tests, and a connection that outlives its bound because the game clock did not
 * move is exactly the failure this bound exists to prevent. Tests shorten the
 * bound instead, via `actionsStreamTimeoutMs`.
 */

import {
  ACTIONS_STREAM_TIMEOUT_MS,
  actionsControlFrame,
  actionsDataFrame,
} from "../../src/application/actions-stream.ts";
import type { AgentMessage } from "../game/action-notifier.ts";

export interface ActionsPage {
  messages: AgentMessage[];
  nextOffset: string;
  upToDate: boolean;
}

export interface ActionsStreamOptions {
  /** Read the player's stream from `cursor`, blocking up to `waitMs`. */
  read(cursor: string | undefined, waitMs: number, signal: AbortSignal): Promise<ActionsPage>;
  /** Opaque resume cursor from the client's previous connection. */
  offset?: string;
  /** The request's own signal: a disconnected client must stop the read loop. */
  signal: AbortSignal;
  timeoutMs?: number;
}

const SEAT_SCOPED_HEADERS = {
  "content-type": "text/event-stream",
  // Never cached and never referred out — the same contract every other
  // capability-gated seat resource carries.
  "cache-control": "no-store",
  "referrer-policy": "no-referrer",
  connection: "keep-alive",
  // Proxies that buffer a response defeat the point of streaming it.
  "x-accel-buffering": "no",
};

export function actionsStreamResponse(options: ActionsStreamOptions): Response {
  const timeoutMs = options.timeoutMs ?? ACTIONS_STREAM_TIMEOUT_MS;
  const startedAt = Date.now();
  const encoder = new TextEncoder();
  const reads = new AbortController();
  let stop = () => reads.abort();

  const body = new ReadableStream<Uint8Array>({
    start: async (controller) => {
      let inactive = false;
      /**
       * The bound is a real timer, not a value recomputed between reads. A read
       * that never settles — a wedged storage call, a live read that loses its
       * wakeup — would otherwise hold the connection open indefinitely, since
       * the loop only re-checks the clock after `read` returns.
       */
      const wall = setTimeout(() => stop(), timeoutMs);
      stop = () => {
        if (inactive) return;
        inactive = true;
        clearTimeout(wall);
        reads.abort();
        try {
          controller.close();
        } catch {
          // The client may already have dropped the body. An ordinary
          // disconnect, not an error worth reporting.
        }
      };
      const onClientGone = () => stop();
      options.signal.addEventListener("abort", onClientGone, { once: true });

      const write = (frame: string): boolean => {
        if (inactive) return false;
        try {
          controller.enqueue(encoder.encode(frame));
          return true;
        } catch {
          stop();
          return false;
        }
      };

      try {
        let cursor = options.offset;
        // The first read is immediate: backlog before blocking.
        let waitMs = 0;
        for (;;) {
          if (inactive) return;
          // A client that gave up before the first read gets a closed stream,
          // not a dangling one.
          if (options.signal.aborted) {
            stop();
            return;
          }
          const page = await options.read(cursor, waitMs, reads.signal);
          if (inactive) return;
          cursor = page.nextOffset;
          // `GameOver` is terminal for this seat. Say so on the control event
          // and close, rather than holding a connection nothing will ever fill.
          const closed = page.messages.at(-1)?.type === "GameOver";
          if (page.messages.length > 0 && !write(actionsDataFrame(page.messages))) return;
          const control = {
            nextOffset: cursor,
            upToDate: page.upToDate,
            ...(closed ? { closed: true } : {}),
          };
          if (!write(actionsControlFrame(control))) return;
          if (closed) {
            stop();
            return;
          }
          waitMs = timeoutMs - (Date.now() - startedAt);
          if (waitMs <= 0) {
            stop();
            return;
          }
        }
      } catch (error) {
        if (!inactive && !reads.signal.aborted) console.error("actions stream error:", error);
        stop();
      } finally {
        clearTimeout(wall);
        options.signal.removeEventListener("abort", onClientGone);
      }
    },
    cancel: () => stop(),
  });

  return new Response(body, { headers: SEAT_SCOPED_HEADERS });
}
