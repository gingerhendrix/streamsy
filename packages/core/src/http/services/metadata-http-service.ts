import type { BoundHttpRouteContext } from "../types.ts";
import { CACHE_NO_STORE, HttpResponseFactory } from "../responses.ts";

export class MetadataHttpService {
  constructor(private deps: { responses: HttpResponseFactory }) {}

  async execute(ctx: BoundHttpRouteContext): Promise<Response> {
    const result = await ctx.stream.metadata();
    if (result.status === "not-found") return this.deps.responses.notFound();
    if (result.status === "gone") return this.deps.responses.gone();
    return this.deps.responses.empty(200, {
      "content-type": result.contentType,
      "stream-next-offset": result.nextOffset,
      ...(result.ttlSeconds ? { "stream-ttl": String(result.ttlSeconds) } : {}),
      ...(result.expiresAt ? { "stream-expires-at": result.expiresAt } : {}),
      ...(result.closed ? { "stream-closed": "true" } : {}),
      "cache-control": CACHE_NO_STORE,
    });
  }
}
