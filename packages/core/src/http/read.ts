import { Clock, Effect, Option, Random } from "effect";
import { HttpServerResponse } from "effect/http";
import type { Reader } from "../protocol/tags.ts";
import type { StreamsFault } from "../fault.ts";
import type { StreamId } from "../schema/index.ts";
import { isValid } from "../offset/index.ts";
import * as Etags from "./etag-builder.ts";
import * as MessageBody from "./message-body-codec.ts";
import { readQueryParser } from "./read-query-parser.ts";
import * as Responses from "./responses.ts";
import { sse } from "./sse.ts";
import type { ReadNextError } from "../protocol/errors.ts";
import type { ReadNextResult } from "../protocol/results.ts";
import { generateCursor } from "../policy/cursor-generator.ts";
import { ShutdownSignal, type ShutdownSignalService } from "./shutdown-signal.ts";

const responses = Responses;

const bodyCodec = MessageBody;

const etags = Etags;

const queryParser = readQueryParser(isValid);

/**
 * The reply a parked long poll gives when the host drains: no messages, the
 * requested offset, and a fresh cursor. The HTTP layer turns it into the same
 * `204` as a long-poll timeout, so a client reconnects as it always does. The
 * next read, on this host or the next one, returns anything that arrived.
 */
const drainReply = Effect.fn("Http.drainReply")(function* (
  reader: Reader<StreamsFault>,
  id: StreamId,
  offset: string,
  cursor: string | undefined,
) {
  const head = yield* reader.head(id);
  const now = yield* Clock.currentTimeMillis;
  const random = yield* Random.next;

  return {
    contentType: head.contentType,
    messages: [],
    nextOffset: offset === "now" ? head.nextOffset : offset,
    upToDate: true,
    closed: false,
    timedOut: true,
    cursor: generateCursor({ now: () => now }, cursor, () => random),
  } satisfies ReadNextResult;
});

/** Park on `readNext` until data, timeout, or the host's drain signal. */
const longPoll = (
  reader: Reader<StreamsFault>,
  id: StreamId,
  offset: string,
  cursor: string | undefined,
  signal: Option.Option<ShutdownSignalService>,
) => {
  const wait = reader.readNext(id, { offset, cursor });

  if (Option.isNone(signal)) return wait;

  const drained = signal.value.draining.pipe(
    Effect.andThen(drainReply(reader, id, offset, cursor)),
  );

  return signal.value.track(Effect.raceFirst(wait, drained));
};

export const read = Effect.fn("Http.read")(function* (
  reader: Reader<StreamsFault>,
  id: StreamId,
  url: URL,
  requestHeaders: Headers,
  cacheControl: string,
  sseDeadlineMs?: number,
): Effect.fn.Return<
  Response | HttpServerResponse.HttpServerResponse,
  StreamsFault | ReadNextError
> {
  const query = queryParser.parse(url);

  if (!query.ok) return query.response;
  const offset = query.offset;

  if (offset === "now" && !query.live) {
    const result = yield* reader.read(id, { offset: "now" });

    const headers = new Headers({
      "content-type": result.contentType,
      "stream-next-offset": result.nextOffset,
      "stream-up-to-date": "true",
      "cache-control": "no-store",
    });

    if (result.closed) headers.set("stream-closed", "true");

    return new Response(bodyCodec.emptyBodyForContentType(result.contentType), { headers });
  }

  if (query.live && !offset) return responses.badRequest("offset required for live modes");
  const signal = yield* Effect.serviceOption(ShutdownSignal);

  if (query.live === "sse" && offset) {
    const meta = yield* reader.head(id);

    return sse(
      reader,
      id,
      meta.contentType,
      offset === "now" ? meta.nextOffset : offset,
      query.cursor,
      sseDeadlineMs,
      Option.getOrUndefined(signal),
    );
  }

  const live = query.live === "long-poll" && offset !== undefined;

  const result =
    live && offset !== undefined
      ? yield* longPoll(reader, id, offset, query.cursor, signal)
      : yield* reader
          .read(id, { offset })
          .pipe(Effect.map((batch) => ({ ...batch, cursor: undefined })));

  const headers = new Headers({ "stream-next-offset": result.nextOffset });

  if (result.closed) headers.set("stream-closed", "true");

  if (live || result.upToDate) headers.set("stream-up-to-date", "true");

  if (live && !result.closed && result.cursor !== undefined)
    headers.set("stream-cursor", result.cursor);

  if (live && result.messages.length === 0) {
    headers.set("cache-control", "no-store");

    return responses.empty(204, headers);
  }

  const etag = etags.forCatchUp(
    url.pathname,
    offset ?? "-1",
    result.nextOffset,
    result.closed === true,
  );

  const literalNow = live && query.offset === "now";
  headers.set("cache-control", literalNow ? "no-store" : cacheControl);

  if (!literalNow) {
    if (requestHeaders.get("if-none-match") === etag)
      return responses.empty(304, { etag, "cache-control": cacheControl });
    headers.set("etag", etag);
  }

  headers.set("content-type", result.contentType);

  return new Response(bodyCodec.encodeHttpBody(result.messages, result.contentType), { headers });
});
