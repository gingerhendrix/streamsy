import type { BoundHttpRouteContext } from "../types.ts";
import { EtagBuilder } from "../etag-builder.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { ReadQueryParser } from "../read-query-parser.ts";
import { CACHE_NO_STORE, HttpResponseFactory } from "../responses.ts";
import { LongPollHttpService } from "./long-poll-http-service.ts";
import { SseHttpService } from "./sse-http-service.ts";

export class ReadHttpService {
  constructor(
    private deps: {
      responses: HttpResponseFactory;
      bodyCodec: MessageBodyCodec;
      readQuery: ReadQueryParser;
      etags: EtagBuilder;
      longPoll: LongPollHttpService;
      sse: SseHttpService;
      cacheControl: string;
    },
  ) {}

  async execute(ctx: BoundHttpRouteContext): Promise<Response> {
    const query = this.deps.readQuery.parse(ctx.url);
    if (!query.ok) return query.response;
    let effectiveOffset = query.offset;
    if (query.offset === "now") {
      const now = await this.resolveNowOffset(ctx, query.live);
      if (!now.ok) return now.response;
      effectiveOffset = now.offset;
      if (now.response) return now.response;
    }
    if (query.live) {
      if (!effectiveOffset) return this.deps.responses.badRequest("offset required for live modes");
      return query.live === "sse"
        ? this.deps.sse.execute(ctx.stream, effectiveOffset, query.cursor)
        : this.deps.longPoll.execute(ctx, effectiveOffset, query.cursor, query.offset === "now");
    }
    return this.handleCatchUp(ctx, effectiveOffset, query.offset ?? "-1", query.batchSize);
  }

  private async resolveNowOffset(
    ctx: BoundHttpRouteContext,
    live?: string,
  ): Promise<
    { ok: true; offset: string; response?: Response } | { ok: false; response: Response }
  > {
    const meta = await ctx.stream.metadata();
    if (meta.status === "not-found") return { ok: false, response: this.deps.responses.notFound() };
    if (meta.status === "gone") return { ok: false, response: this.deps.responses.gone() };
    const offset = meta.nextOffset;
    if (!live) {
      const read = await ctx.stream.read({ offset: "now" });
      if (read.status === "not-found")
        return { ok: false, response: this.deps.responses.notFound() };
      if (read.status === "gone") return { ok: false, response: this.deps.responses.gone() };
      const contentType = meta.contentType;
      const headers = new Headers({
        "content-type": contentType,
        "stream-next-offset": read.nextOffset,
        "stream-up-to-date": "true",
        "cache-control": CACHE_NO_STORE,
      });
      if (read.closed) headers.set("stream-closed", "true");
      return {
        ok: true,
        offset: read.nextOffset,
        response: new Response(this.deps.bodyCodec.emptyBodyForContentType(contentType), {
          headers,
        }),
      };
    }
    return { ok: true, offset };
  }

  private async handleCatchUp(
    ctx: BoundHttpRouteContext,
    offset: string | undefined,
    startOffset: string,
    batchSize: number | undefined,
  ): Promise<Response> {
    const result = await ctx.stream.read({ offset, limit: batchSize });
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    const etag = this.deps.etags.forCatchUp(
      ctx.url.pathname,
      startOffset,
      result.nextOffset,
      result.closed === true,
    );
    if (ctx.request.headers.get("if-none-match") === etag) {
      return this.deps.responses.empty(304, { etag, "cache-control": this.deps.cacheControl });
    }
    const metadata = await ctx.stream.metadata();
    if (metadata.status === "not-found") return this.deps.responses.notFound();
    if (metadata.status === "gone") return this.deps.responses.gone();
    const headers = new Headers({
      "content-type": metadata.contentType,
      "stream-next-offset": result.nextOffset,
      etag,
      "cache-control": this.deps.cacheControl,
    });
    if (result.upToDate) headers.set("stream-up-to-date", "true");
    if (result.closed) headers.set("stream-closed", "true");
    return new Response(this.deps.bodyCodec.encodeHttpBody(result.messages, metadata.contentType), {
      headers,
    });
  }
}
