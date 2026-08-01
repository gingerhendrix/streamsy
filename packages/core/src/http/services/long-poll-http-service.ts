import type { BoundHttpRouteContext } from "../types.ts";
import { EtagBuilder } from "../etag-builder.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { maybeNotSupportedResponse } from "../not-supported.ts";
import { CACHE_NO_STORE, HttpResponseFactory } from "../responses.ts";

export class LongPollHttpService {
  constructor(
    private deps: {
      responses: HttpResponseFactory;
      bodyCodec: MessageBodyCodec;
      etags: EtagBuilder;
      cacheControl: string;
    },
  ) {}

  async execute(
    ctx: BoundHttpRouteContext,
    offset: string,
    cursor?: string,
    literalNow = false,
  ): Promise<Response> {
    const result = await ctx.stream.readLive({
      offset,
      mode: "long-poll",
      cursor,
    });
    if (result.status === "not-supported")
      return maybeNotSupportedResponse(result, this.deps.responses)!;
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    if (result.messages.length === 0) return this.toNoContentResponse(result);
    const metadata = await ctx.stream.metadata();
    if (metadata.status === "not-found") return this.deps.responses.notFound();
    if (metadata.status === "gone") return this.deps.responses.gone();
    const etag = this.deps.etags.forCatchUp(
      ctx.url.pathname,
      offset,
      result.nextOffset,
      result.closed === true,
    );
    const cacheControl = literalNow ? CACHE_NO_STORE : this.deps.cacheControl;
    if (!literalNow && ctx.request.headers.get("if-none-match") === etag) {
      return this.deps.responses.empty(304, { etag, "cache-control": cacheControl });
    }
    return new Response(this.deps.bodyCodec.encodeHttpBody(result.messages, metadata.contentType), {
      headers: {
        "content-type": metadata.contentType,
        "stream-next-offset": result.nextOffset,
        "stream-up-to-date": "true",
        ...(result.closed ? {} : { "stream-cursor": result.cursor }),
        ...(result.closed ? { "stream-closed": "true" } : {}),
        ...(!literalNow ? { etag } : {}),
        "cache-control": cacheControl,
      },
    });
  }

  private toNoContentResponse(result: {
    nextOffset: string;
    cursor: string;
    closed?: boolean;
  }): Response {
    return this.deps.responses.empty(204, {
      "stream-next-offset": result.nextOffset,
      "stream-up-to-date": "true",
      ...(result.closed ? {} : { "stream-cursor": result.cursor }),
      ...(result.closed ? { "stream-closed": "true" } : {}),
      "cache-control": CACHE_NO_STORE,
    });
  }
}
