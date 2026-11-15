import type { DurableObjectState, WebSocket } from "@cloudflare/workers-types";
import { Array, Effect, Option, PubSub, Schema, type Scope, Semaphore } from "effect";
import { Storage } from "@streamsy/core";
import {
  LIVE_SOCKET_PROTOCOL,
  LiveCloseCode,
  closeCodeForStatus,
  controlFrame,
  isLiveUpgrade,
  nextCursor,
  openFrame,
  securityHeaders,
  streamPath,
  upgradeRequired,
} from "@streamsy/core/http";

/**
 * Live reads over hibernatable WebSockets, a Streamsy extension to Durable
 * Streams HTTP. The object accepts the socket with `acceptWebSocket`, keeps
 * each socket's offset in its attachment, and pushes after each commit from
 * the event that committed. Nothing on this path parks a fiber or starts a
 * timer, so an object whose readers all hold sockets can hibernate.
 */
export interface LiveSocketOptions {
  /** The prefix of the stream routes, as in `Http.routes` and `router`. Default `/`. */
  readonly prefix?: `/${string}`;
  /** Open sockets per object. A connect over the limit gets `503`. Default 1,000. */
  readonly maxSockets?: number;
  /** Data bytes a socket receives in one event before it closes with `4100`. Default 1 MiB. */
  readonly maxPushBytes?: number;
  /** Data bytes a connect sends before it closes with `4100`. Default 4 MiB. */
  readonly maxCatchUpBytes?: number;
}

export interface LiveConfiguration {
  readonly websocket: true | LiveSocketOptions;
}

export interface LiveSockets {
  /** True for an upgrade to a stream under the configured prefix. */
  readonly accepts: (request: Request) => boolean;
  readonly connect: (request: Request) => Effect.Effect<Response>;
  /** Pushes every commit since the last settle. Run it at the end of each event. */
  readonly settle: Effect.Effect<void>;
}

/** The tag on every live socket. */
export const LIVE_TAG = "streamsy:live";

/** The platform limit of hibernatable sockets per object. */
const PLATFORM_SOCKET_LIMIT = 32_768;

/** Leaves room under the 16 KiB attachment limit for later fields. */
const MAX_ATTACHMENT_BYTES = 12 * 1024;

const MAX_TAG_LENGTH = 256;

const UPGRADE_HEADERS = [
  "upgrade",
  "connection",
  "sec-websocket-key",
  "sec-websocket-version",
  "sec-websocket-extensions",
  "sec-websocket-protocol",
  "if-none-match",
];

const encoder = new TextEncoder();

/** Version 1 of the per-socket state. A socket with another version closes with `1012`. */
const Attachment = Schema.Struct({
  v: Schema.Literal(1),
  id: Schema.String,
  /** The stream URL without its query. Pushes read through it. */
  url: Schema.String,
  offset: Schema.String,
  cursor: Schema.String,
  contentType: Schema.String,
});

interface Attachment extends Schema.Schema.Type<typeof Attachment> {}

const decodeAttachment = Schema.decodeUnknownOption(Attachment);

const encodeAttachmentJson = Schema.encodeEffect(Schema.fromJsonString(Attachment));

interface Settings {
  readonly path: ReturnType<typeof streamPath>;
  readonly maxSockets: number;
  readonly maxPushBytes: number;
  readonly maxCatchUpBytes: number;
}

const positive = (name: string, value: number, limit = Number.MAX_SAFE_INTEGER): number => {
  if (!Number.isSafeInteger(value) || value <= 0 || value > limit)
    throw new RangeError(`${name} must be a positive integer up to ${limit}`);

  return value;
};

/** Checks the options when the object class is made. */
export function liveSettings(configuration: LiveConfiguration): Settings {
  const options = configuration.websocket === true ? {} : configuration.websocket;

  return {
    path: streamPath(options.prefix ?? "/"),
    maxSockets: positive("maxSockets", options.maxSockets ?? 1_000, PLATFORM_SOCKET_LIMIT),
    maxPushBytes: positive("maxPushBytes", options.maxPushBytes ?? 1024 * 1024),
    maxCatchUpBytes: positive("maxCatchUpBytes", options.maxCatchUpBytes ?? 4 * 1024 * 1024),
  };
}

/** `s:<id>`, or `h:<sha-256 hex>` when that exceeds the tag limit. Pushes compare the full id. */
export const streamTag = Effect.fn("LiveSocket.streamTag")(function* (id: string) {
  const plain = `s:${id}`;

  if (plain.length <= MAX_TAG_LENGTH) return plain;

  const digest = yield* Effect.promise(() => crypto.subtle.digest("SHA-256", encoder.encode(id)));

  return `h:${Array.join(
    Array.map(Array.fromIterable(new Uint8Array(digest)), (byte) =>
      byte.toString(16).padStart(2, "0"),
    ),
    "",
  )}`;
});

/** One catch-up page, read through the object's own HTTP handler. */
interface Page {
  readonly status: number;
  readonly response: Response;
  readonly body: Uint8Array;
  readonly empty: boolean;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
  readonly contentType: string;
}

const readPage = Effect.fn("LiveSocket.readPage")(function* (
  handler: (request: Request) => Promise<Response>,
  request: Request,
) {
  const response = yield* Effect.promise(() => handler(request));

  if (response.status !== 200) return { status: response.status, response } as const;

  const body = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
  const contentType = response.headers.get("content-type") ?? "application/octet-stream";
  const json = contentType.toLowerCase().startsWith("application/json");

  const page: Page = {
    status: 200,
    response,
    body,
    // An empty JSON batch is the two bytes `[]`.
    empty: body.byteLength === 0 || (json && body.byteLength === 2 && body[0] === 0x5b),
    nextOffset: response.headers.get("stream-next-offset") ?? "",
    upToDate: response.headers.get("stream-up-to-date") === "true",
    closed: response.headers.get("stream-closed") === "true",
    contentType,
  };

  return page;
});

const controlOf = (page: Page, cursor: string): string =>
  controlFrame({
    nextOffset: page.nextOffset,
    cursor,
    upToDate: page.upToDate,
    closed: page.closed,
  });

const isPage = (value: { readonly status: number }): value is Page => value.status === 200;

/** A non-live catch-up `GET` at `offset` for the stream at `url`. */
const catchUpRequest = (url: string, offset: string, headers?: Headers): Request => {
  const target = new URL(url);
  target.searchParams.delete("live");
  target.searchParams.set("offset", offset);

  return new Request(target.href, { method: "GET", headers });
};

const send = (ws: WebSocket, frame: string | Uint8Array) =>
  Effect.sync(() => {
    try {
      ws.send(frame);
    } catch {
      // The client already closed. Its close event ends the socket.
    }
  });

const close = (ws: WebSocket, code: number, reason: string) =>
  Effect.sync(() => {
    try {
      ws.close(code, reason);
    } catch {
      // The socket is already closing.
    }
  });

const textResponse = (message: string, status: number, headers?: Record<string, string>) =>
  new Response(message, { status, headers: { ...securityHeaders, ...headers } });

type Frame = string | Uint8Array;

/**
 * Subscribes to the object's commits in the acquiring scope. The subscription
 * is a queue that `settle` drains without waiting, so no fiber parks on it.
 */
export const makeLiveSockets = Effect.fn("LiveSocket.make")(function* (
  state: DurableObjectState,
  handler: (request: Request) => Promise<Response>,
  settings: Settings,
): Effect.fn.Return<LiveSockets, never, Storage | Scope.Scope> {
  const storage = yield* Storage;
  const committed = yield* storage.committed;
  // Connects and pushes take turns, so a commit is either in a connect's
  // catch-up or pushed after the socket's offset is stored.
  const permit = yield* Semaphore.make(1);

  const streamId = (request: Request): Option.Option<string> => {
    const pathname = new URL(request.url).pathname;
    const id = settings.path.strip(pathname);

    return id.length > 0 && id !== pathname ? Option.some(id) : Option.none();
  };

  const accepts = (request: Request) => isLiveUpgrade(request) && Option.isSome(streamId(request));

  const connect = (request: Request) =>
    permit.withPermit(
      Effect.gen(function* () {
        const url = new URL(request.url);
        const match = streamId(request);

        if (Option.isNone(match)) return upgradeRequired();
        const id = match.value;
        const requested = url.searchParams.get("offset");

        if (requested === null) return textResponse("offset required for live modes", 400);

        const headers = new Headers(request.headers);

        for (const name of UPGRADE_HEADERS) headers.delete(name);

        const base = `${url.origin}${url.pathname}`;
        const query = new URL(base);
        const cursorParam = url.searchParams.get("cursor");

        if (cursorParam !== null) query.searchParams.set("cursor", cursorParam);

        // The first read runs routes, access, middleware and offset checks as a
        // catch-up `GET` would, and its error is the answer to the upgrade.
        const first = yield* readPage(handler, catchUpRequest(query.href, requested, headers));

        if (!isPage(first)) return first.response;

        if (state.getWebSockets(LIVE_TAG).length >= settings.maxSockets)
          return textResponse("Too many live sockets", 503, { "retry-after": "1" });

        const frames: Array<Frame> = [openFrame(first.contentType, first.nextOffset)];
        let cursor = cursorParam ?? "";
        let page: Page = first;
        let offset = requested;
        let sent = 0;
        let closeCode: number | undefined;

        for (;;) {
          if (!page.empty) {
            if (sent + page.body.byteLength > settings.maxCatchUpBytes) {
              // The client resumes over HTTP from the last control, so one is always sent.
              if (offset === requested) {
                cursor = yield* nextCursor(cursor);
                frames.push(
                  controlFrame({ nextOffset: offset, cursor, upToDate: false, closed: false }),
                );
              }

              closeCode = LiveCloseCode.CatchUpRequired;
              break;
            }

            sent += page.body.byteLength;
            frames.push(page.body);
          }

          cursor = yield* nextCursor(cursor);
          offset = page.nextOffset;
          frames.push(controlOf(page, cursor));

          if (page.closed) {
            closeCode = LiveCloseCode.StreamClosed;
            break;
          }

          if (page.upToDate) break;

          const next = yield* readPage(handler, catchUpRequest(base, offset, headers));

          if (!isPage(next)) {
            closeCode = closeCodeForStatus(next.status);
            break;
          }

          page = next;
        }

        const attachment: Attachment = {
          v: 1,
          id,
          url: base,
          offset,
          cursor,
          contentType: first.contentType,
        };

        const serialized = yield* encodeAttachmentJson(attachment).pipe(Effect.orDie);

        if (encoder.encode(serialized).byteLength > MAX_ATTACHMENT_BYTES)
          return textResponse("Stream URL too long for a live socket", 414);

        const tag = yield* streamTag(id);
        const pair = new WebSocketPair();
        const client = pair[0];
        const server = pair[1];
        state.acceptWebSocket(server, [LIVE_TAG, tag]);
        server.serializeAttachment(attachment);

        for (const frame of frames) yield* send(server, frame);

        if (closeCode !== undefined) yield* close(server, closeCode, "");

        const offered = Array.map(
          (request.headers.get("sec-websocket-protocol") ?? "").split(","),
          (name) => name.trim(),
        );

        return new Response(null, {
          status: 101,
          webSocket: client,
          headers: offered.includes(LIVE_SOCKET_PROTOCOL)
            ? { "sec-websocket-protocol": LIVE_SOCKET_PROTOCOL }
            : {},
        });
      }),
    );

  interface Member {
    readonly ws: WebSocket;
    readonly attachment: Attachment;
  }

  // Every socket in a group is at one offset, so one read serves them all.
  const pushGroup = Effect.fn("LiveSocket.pushGroup")(function* (group: ReadonlyArray<Member>) {
    const [head] = group;

    if (head === undefined) return;

    const sockets = Array.map(group, (member) => member.ws);
    const broadcast = (frame: Frame) => Effect.forEach(sockets, (ws) => send(ws, frame));

    const closeAll = (code: number, reason: string) =>
      Effect.forEach(sockets, (ws) => close(ws, code, reason));

    let offset = head.attachment.offset;
    let cursor = head.attachment.cursor;
    let sent = 0;

    for (;;) {
      const page = yield* readPage(handler, catchUpRequest(head.attachment.url, offset));

      if (!isPage(page)) {
        yield* closeAll(closeCodeForStatus(page.status), "");

        return;
      }

      // A commit that added nothing to this stream, such as a producer retry.
      if (page.empty && !page.closed && page.nextOffset === offset) break;

      if (!page.empty) {
        if (sent + page.body.byteLength > settings.maxPushBytes) {
          yield* closeAll(LiveCloseCode.CatchUpRequired, "");

          return;
        }

        sent += page.body.byteLength;
        yield* broadcast(page.body);
      }

      cursor = yield* nextCursor(cursor);
      offset = page.nextOffset;
      yield* broadcast(controlOf(page, cursor));

      if (page.closed) {
        yield* closeAll(LiveCloseCode.StreamClosed, "");

        return;
      }

      if (page.upToDate) break;
    }

    for (const { ws, attachment } of group) {
      const moved: Attachment = {
        v: 1,
        id: attachment.id,
        url: attachment.url,
        offset,
        cursor,
        contentType: attachment.contentType,
      };

      ws.serializeAttachment(moved);
    }
  });

  const pushStream = Effect.fn("LiveSocket.pushStream")(function* (id: string) {
    const groups = new Map<string, Array<Member>>();

    for (const ws of state.getWebSockets(yield* streamTag(id))) {
      const attachment = decodeAttachment(ws.deserializeAttachment());

      if (Option.isNone(attachment)) {
        yield* close(ws, LiveCloseCode.Restart, "Unknown socket state");
        continue;
      }

      // A hashed tag can collide. The full id decides.
      if (attachment.value.id !== id) continue;
      const group = groups.get(attachment.value.offset) ?? [];
      group.push({ ws, attachment: attachment.value });
      groups.set(attachment.value.offset, group);
    }

    for (const group of groups.values())
      yield* pushGroup(group).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Live socket push failed", cause).pipe(
            Effect.andThen(
              Effect.forEach(group, (member) => close(member.ws, LiveCloseCode.InternalError, "")),
            ),
          ),
        ),
      );
  });

  const settle = permit.withPermit(
    Effect.gen(function* () {
      const batches = yield* PubSub.takeUpTo(committed, Number.POSITIVE_INFINITY);

      if (batches.length === 0 || state.getWebSockets(LIVE_TAG).length === 0) return;

      for (const id of new Set(Array.flatten(batches))) yield* pushStream(id);
    }).pipe(
      // The event's own response stands. Its readers reconnect or get the next push.
      Effect.catchCause((cause) => Effect.logError("Live socket settle failed", cause)),
    ),
  );

  return { accepts, connect, settle };
});
