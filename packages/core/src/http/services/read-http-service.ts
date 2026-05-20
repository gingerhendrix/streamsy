import type { StreamProtocolInterface } from "../../types/protocol.ts";
import type { HttpRouteContext } from "../types.ts";
import { EtagBuilder } from "../etag-builder.ts";
import { MessageBodyCodec } from "../message-body-codec.ts";
import { ReadQueryParser } from "../read-query-parser.ts";
import { CACHE_NO_STORE, CACHE_REVALIDATE, HttpResponseFactory } from "../responses.ts";
import { LongPollHttpService } from "./long-poll-http-service.ts";
import { SseHttpService } from "./sse-http-service.ts";

export class ReadHttpService {
  constructor(
    private deps: { protocol: StreamProtocolInterface; responses: HttpResponseFactory; bodyCodec: MessageBodyCodec; readQuery: ReadQueryParser; etags: EtagBuilder; longPoll: LongPollHttpService; sse: SseHttpService },
  ) {}

  async execute(ctx: HttpRouteContext): Promise<Response> {
    const query = this.deps.readQuery.parse(ctx.url);
    if (!query.ok) return query.response;
    let effectiveOffset = query.offset;
    if (query.offset === "now") {
      const now = await this.resolveNowOffset(ctx.streamId, query.live);
      if (!now.ok) return now.response;
      effectiveOffset = now.offset;
      if (now.response) return now.response;
    }
    if (query.live) {
      if (!effectiveOffset) return this.deps.responses.badRequest("offset required for live modes");
      return query.live === "sse"
        ? this.deps.sse.execute(ctx.streamId, effectiveOffset, query.cursor)
        : this.deps.longPoll.execute(ctx.streamId, effectiveOffset, query.cursor);
    }
    return this.handleCatchUp(ctx, effectiveOffset, query.offset ?? "-1");
  }

  private async resolveNowOffset(streamId: string, live?: string): Promise<{ ok: true; offset: string; response?: Response } | { ok: false; response: Response }> {
    const meta = await this.deps.protocol.metadata(streamId);
    if (meta.status === "not-found") return { ok: false, response: this.deps.responses.notFound() };
    const offset = meta.nextOffset!;
    if (!live) {
      const contentType = meta.contentType!;
      return { ok: true, offset, response: new Response(this.deps.bodyCodec.emptyBodyForContentType(contentType), { headers: { "content-type": contentType, "stream-next-offset": offset, "stream-up-to-date": "true", ...(meta.closed ? { "stream-closed": "true" } : {}), "cache-control": CACHE_NO_STORE } }) };
    }
    return { ok: true, offset };
  }

  private async handleCatchUp(ctx: HttpRouteContext, offset: string | undefined, startOffset: string): Promise<Response> {
    const result = await this.deps.protocol.read(ctx.streamId, { offset });
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    const etag = this.deps.etags.forCatchUp(ctx.url.pathname, startOffset, result.nextOffset, result.closed === true);
    if (ctx.request.headers.get("if-none-match") === etag) {
      return this.deps.responses.empty(304, { etag, "cache-control": CACHE_REVALIDATE });
    }
    const metadata = await this.deps.protocol.metadata(ctx.streamId);
    return new Response(this.deps.bodyCodec.encodeHttpBody(result.messages, metadata.contentType!), { headers: { "content-type": metadata.contentType!, "stream-next-offset": result.nextOffset, ...(result.upToDate ? { "stream-up-to-date": "true" } : {}), ...(result.closed ? { "stream-closed": "true" } : {}), etag, "cache-control": CACHE_REVALIDATE } });
  }
}
