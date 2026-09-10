import { format } from "../protocol/remote-format.ts";
import { outcomeResponse } from "./outcome-response.ts";
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
import { notSupported } from "./unsupported.ts";

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
): Effect.fn.Return<Response | HttpServerResponse.HttpServerResponse, StreamsFault> {
  const query = queryParser.parse(url);
  if (!query.ok) return query.response;
  if (requestHeaders.get("accept") === format) {
    const result =
      query.live === "long-poll" && query.offset !== undefined
        ? yield* reader.readNext(id, { offset: query.offset, cursor: query.cursor })
        : yield* reader.read(id, { offset: query.offset, limit: query.batchSize });
    const status =
      result.status === "not-found"
        ? 404
        : result.status === "gone"
          ? 410
          : result.status === "not-supported"
            ? 400
            : 200;
    return outcomeResponse(result, responses.empty(status));
  }
  let offset = query.offset;
  if (offset === "now") {
    const meta = yield* reader.head(id);
    if (meta.status === "not-found") return responses.notFound();
    if (meta.status === "gone") return responses.gone();
    offset = meta.nextOffset;
    if (!query.live) {
      const result = yield* reader.read(id, { offset: "now" });
      if (result.status === "not-found") return responses.notFound();
      if (result.status === "gone") return responses.gone();
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
    if (meta.status === "not-found") return responses.notFound();
    if (meta.status === "gone") return responses.gone();
    return sse(reader, id, meta.contentType, offset, query.cursor);
  }
  const live = query.live === "long-poll" && offset !== undefined;
  const result =
    live && offset !== undefined
      ? yield* reader.readNext(id, { offset, cursor: query.cursor })
      : yield* reader.read(id, { offset, limit: query.batchSize });
  if (result.status === "not-supported") return notSupported(result);
  if (result.status === "not-found") return responses.notFound();
  if (result.status === "gone") return responses.gone();
  const headers = new Headers({ "stream-next-offset": result.nextOffset });
  if (result.closed) headers.set("stream-closed", "true");
  if (live || result.upToDate) headers.set("stream-up-to-date", "true");
  if (live && !result.closed && "cursor" in result) headers.set("stream-cursor", result.cursor);
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
  if (meta.status === "not-found") return responses.notFound();
  if (meta.status === "gone") return responses.gone();
  headers.set("content-type", meta.contentType);
  return new Response(bodyCodec.encodeHttpBody(result.messages, meta.contentType), { headers });
});
