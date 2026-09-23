import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamsReader } from "../protocol/tags.ts";
import type { StreamId } from "../schema/index.ts";
import { read as readProtocol } from "./read.ts";
import { requestUrl } from "./route.ts";
import { cacheControlForVisibility, fromWeb } from "./responses.ts";
import { securityHeaders } from "./security-headers.ts";

export interface ReadOptions {
  readonly sseDeadlineMs?: number;
  readonly cacheVisibility?: "private" | "public";
}

/** Read an already resolved id using the protocol's HTTP framing and live modes. */
export function read(id: StreamId, options: ReadOptions = {}) {
  if (
    options.sseDeadlineMs !== undefined &&
    (!Number.isFinite(options.sseDeadlineMs) || options.sseDeadlineMs <= 0)
  ) {
    throw new RangeError("sseDeadlineMs must be positive");
  }
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const reader = yield* StreamsReader;
    const url = requestUrl(request);
    if (!url) return HttpServerResponse.text("Invalid request URL", { status: 400 });
    if (request.method === "HEAD") {
      const meta = yield* reader.head(id);
      const headers = new Headers({
        "content-type": meta.contentType,
        "stream-next-offset": meta.nextOffset,
        "cache-control": "no-store",
      });
      if (meta.closed) headers.set("stream-closed", "true");
      if (meta.ttlSeconds) headers.set("stream-ttl", String(meta.ttlSeconds));
      if (meta.expiresAt) headers.set("stream-expires-at", meta.expiresAt);
      return HttpServerResponse.empty({ status: 200, headers: Object.fromEntries(headers) });
    }
    const response = yield* readProtocol(
      reader,
      id,
      url,
      new Headers(request.headers),
      cacheControlForVisibility(options.cacheVisibility ?? "private"),
      options.sseDeadlineMs,
    );
    return response instanceof Response ? fromWeb(response) : response;
  }).pipe(Effect.map(HttpServerResponse.setHeaders(securityHeaders)));
}
