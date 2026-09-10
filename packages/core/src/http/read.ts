import { Effect } from "effect";
import { HttpServerResponse } from "effect/unstable/http";
import type { Reader } from "../protocol/tags.ts";
import type { StreamsFault } from "../fault.ts";
import type { StreamId } from "../schema/index.ts";
import { isValid } from "../offset/index.ts";
import { EtagBuilder } from "./etag-builder.ts";
import { MessageBodyCodec } from "./message-body-codec.ts";
import { ReadQueryParser } from "./read-query-parser.ts";
import { HttpResponseFactory } from "./responses.ts";
import { sse } from "./sse.ts";
import type { ReadNextError } from "../protocol/errors.ts";

const responses = new HttpResponseFactory();
const bodyCodec = new MessageBodyCodec();
const etags = new EtagBuilder();
const queryParser = new ReadQueryParser(isValid);
export const read = Effect.fn("Http.read")(function* (
  reader: Reader<StreamsFault>,
  id: StreamId,
  url: URL,
  requestHeaders: Headers,
  cacheControl: string,
): Effect.fn.Return<
  Response | HttpServerResponse.HttpServerResponse,
  StreamsFault | ReadNextError
> {
  const query = queryParser.parse(url);
  if (!query.ok) return query.response;
  let offset = query.offset;
  if (offset === "now") {
    const meta = yield* reader.head(id);

    offset = meta.nextOffset;
    if (!query.live) {
      const result = yield* reader.read(id, { offset: "now" });

      const headers = new Headers({
        "content-type": meta.contentType,
        "stream-next-offset": result.nextOffset,
        "stream-up-to-date": "true",
        "cache-control": "no-store",
      });
      if (result.closed) headers.set("stream-closed", "true");
      return new Response(bodyCodec.emptyBodyForContentType(meta.contentType), { headers });
    }
  }
  if (query.live && !offset) return responses.badRequest("offset required for live modes");
  if (query.live === "sse" && offset) {
    const meta = yield* reader.head(id);

    return sse(reader, id, meta.contentType, offset, query.cursor);
  }
  const live = query.live === "long-poll" && offset !== undefined;
  const result =
    live && offset !== undefined
      ? yield* reader.readNext(id, { offset, cursor: query.cursor })
      : yield* reader.read(id, { offset, limit: query.batchSize });

  const headers = new Headers({ "stream-next-offset": result.nextOffset });
  if (result.closed) headers.set("stream-closed", "true");
  if (live || result.upToDate) headers.set("stream-up-to-date", "true");
  if (live && !result.closed && "cursor" in result && typeof result.cursor === "string")
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
  const meta = yield* reader.head(id);

  headers.set("content-type", meta.contentType);
  return new Response(bodyCodec.encodeHttpBody(result.messages, meta.contentType), { headers });
});
