import type { BoundHttpRouteContext } from "../types.ts";
import { CACHE_NO_STORE, HttpResponseFactory } from "../responses.ts";

export class MetadataHttpService {
  constructor(private deps: { responses: HttpResponseFactory }) {}

  async execute(ctx: BoundHttpRouteContext): Promise<Response> {
    const result = await ctx.stream.metadata();
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    const headers = new Headers({
      "content-type": result.contentType,
      "stream-next-offset": result.nextOffset,
      "cache-control": CACHE_NO_STORE,
    });
    if (result.ttlSeconds) headers.set("stream-ttl", String(result.ttlSeconds));
    if (result.expiresAt) headers.set("stream-expires-at", result.expiresAt);
    if (result.closed) headers.set("stream-closed", "true");
    return this.deps.responses.empty(200, headers);
  }
}
