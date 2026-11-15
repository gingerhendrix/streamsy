import { Clock, Effect, Random } from "effect";
import { generateCursor } from "../policy/cursor-generator.ts";
import type { SseControlData } from "./sse-event-encoder.ts";

/**
 * Frames of `live=websocket`, a Streamsy extension to Durable Streams HTTP.
 *
 * The server sends one `open` text frame, then a binary `data` frame and a
 * `control` text frame for each batch. A `data` frame holds the exact body of
 * a catch-up `GET` for the batch. A `control` frame has the fields of an SSE
 * control event, and a client moves its offset only on `control`.
 */
export const LIVE_SOCKET_PROTOCOL = "streamsy-live.v1";

/** The only client message. The runtime answers it without running the server. */
export const LIVE_PING = "ping";

export const LIVE_PONG = "pong";

/** The close codes of `streamsy-live.v1`. */
export const LiveCloseCode = {
  /** The stream closed. A `control` frame with `streamClosed` came first. */
  StreamClosed: 1000,
  InternalError: 1011,
  /** The server restarted or cannot read the socket's state. Reconnect at once. */
  Restart: 1012,
  /** The socket fell behind the byte cap. Read over HTTP, then reconnect. */
  CatchUpRequired: 4100,
  BadMessage: 4400,
  NotFound: 4404,
  Gone: 4410,
} as const;

export type LiveCloseCode = (typeof LiveCloseCode)[keyof typeof LiveCloseCode];

export interface LiveOpenFrame {
  readonly type: "open";
  readonly protocol: typeof LIVE_SOCKET_PROTOCOL;
  readonly contentType: string;
  readonly streamNextOffset: string;
}

export interface LiveControlFrame extends SseControlData {
  readonly type: "control";
}

export interface LiveBatch {
  readonly nextOffset: string;
  readonly cursor: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

/** The browser socket API hides response headers, so `open` carries the content type. */
export function openFrame(contentType: string, streamNextOffset: string): string {
  const frame: LiveOpenFrame = {
    type: "open",
    protocol: LIVE_SOCKET_PROTOCOL,
    contentType,
    streamNextOffset,
  };

  return JSON.stringify(frame);
}

/** The SSE control rules: no cursor on a closed stream, `upToDate` only while open. */
export function controlFrame(batch: LiveBatch): string {
  const frame: LiveControlFrame = batch.closed
    ? { type: "control", streamNextOffset: batch.nextOffset, streamClosed: true }
    : { type: "control", streamNextOffset: batch.nextOffset, streamCursor: batch.cursor };

  if (!batch.closed && batch.upToDate) frame.upToDate = true;

  return JSON.stringify(frame);
}

/** The cursor a long-poll response would carry after `previous`. */
export const nextCursor = Effect.fn("Http.nextCursor")(function* (previous: string | undefined) {
  const now = yield* Clock.currentTimeMillis;
  const random = yield* Random.next;

  return generateCursor({ now: () => now }, previous, () => random);
});

/** The close code for a read that failed with an HTTP status. */
export function closeCodeForStatus(status: number): LiveCloseCode {
  if (status === 404) return LiveCloseCode.NotFound;

  if (status === 410) return LiveCloseCode.Gone;

  return LiveCloseCode.InternalError;
}

/** A `GET` that asks for `live=websocket` with an `Upgrade: websocket` header. */
export function isLiveUpgrade(request: Request): boolean {
  return (
    request.method === "GET" &&
    request.headers.get("upgrade")?.toLowerCase() === "websocket" &&
    new URL(request.url).searchParams.get("live") === "websocket"
  );
}

/** The answer to `live=websocket` from a host or request that cannot upgrade. Clients fall back to long-poll. */
export function upgradeRequired(): Response {
  return new Response("WebSocket upgrade required", {
    status: 426,
    headers: { upgrade: "websocket", "cache-control": "no-store" },
  });
}
