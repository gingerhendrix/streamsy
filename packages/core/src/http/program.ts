import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { StreamId } from "../schema/index.ts";
import type { StreamsFault } from "../fault.ts";
import { HttpResponseFactory, cacheControlForVisibility } from "./responses.ts";
import { StreamPathService } from "./stream-path-service.ts";
import { RequestBodyReader } from "./request-body-reader.ts";
import * as Create from "./create.ts";
import * as Append from "./append.ts";
import { read } from "./read.ts";
import { requestUrl } from "./route.ts";
import { protocolErrorResponse } from "./protocol-error-response.ts";
import { isProtocolError, type ProtocolError } from "../protocol/errors.ts";

export interface HttpOptions {
  readonly pathPrefix?: string;
  readonly maxMessageSize?: number;
  readonly cacheVisibility?: "private" | "public";
}

const responses = new HttpResponseFactory();
/**
 * The public HTTP program. It is the whole application an Effect host serves.
 *
 * Its success type is `HttpServerResponse` alone, its error channel is closed,
 * and it requires exactly `HttpServerRequest | StreamsReader | StreamsWriter`.
 * Those three facts are what let a host type it as an Alchemy `HttpEffect` and
 * what let one composition serve several backends.
 */
export function app(options: HttpOptions = {}) {
  const path = new StreamPathService(options.pathPrefix ?? "/");
  const bodyReader = new RequestBodyReader(options.maxMessageSize ?? 1024 * 1024, responses);
  const cacheControl = cacheControlForVisibility(options.cacheVisibility ?? "private");
  const failure = (error: ProtocolError) =>
    Effect.map(HttpServerRequest.HttpServerRequest, (request) =>
      protocolErrorResponse(error, { method: request.method }),
    );
  return Effect.gen(function* (): Effect.fn.Return<
    Response | HttpServerResponse.HttpServerResponse,
    StreamsFault | ProtocolError,
    HttpServerRequest.HttpServerRequest | StreamsReader | StreamsWriter
  > {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const reader = yield* StreamsReader;
    const writer = yield* StreamsWriter;
    const url = requestUrl(request);
    if (!url) return responses.badRequest("Invalid request URL");
    const stripped = path.strip(url.pathname);
    if (!stripped || stripped === url.pathname)
      return responses.badRequest(`Stream path required: ${path.requiredPathPattern()}`);
    const id = StreamId.make(stripped);
    const headers = new Headers(request.headers);
    if (request.method === "PUT") {
      const parsed = Create.parseHeaders({ headers }, path);
      if (!parsed.ok) return parsed.response;
      const body = yield* bodyReader.read(request);
      if (!body.ok) return body.response;
      const normalized = Create.normalizeInitialData(body.data, parsed.contentType);
      if (!normalized.ok) return normalized.response;
      const result = yield* writer.create(id, {
        contentType: parsed.contentType,
        ttlSeconds: parsed.ttlSeconds,
        expiresAt: parsed.expiresAt,
        initialData: normalized.initialData,
        closed: parsed.wantClosed,
        forkedFrom: parsed.forkedFromStreamId,
        forkOffset: parsed.forkOffset,
        forkSubOffset: parsed.forkSubOffset,
      });
      return Create.toResponse(result, url.href);
    }
    if (request.method === "OPTIONS")
      return responses.empty(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, HEAD, PUT, POST, DELETE, OPTIONS",
        "access-control-allow-headers":
          "Authorization, Content-Type, If-None-Match, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Closed, Stream-Expected-Offset, Stream-Seq, Stream-TTL, Stream-Expires-At",
        "access-control-max-age": "86400",
      });
    if (!["POST", "GET", "HEAD", "DELETE"].includes(request.method))
      return responses.methodNotAllowed();
    const meta = yield* reader.head(id);
    switch (request.method) {
      case "POST": {
        const parsed = Append.parseHeaders({ headers });
        if (!parsed.ok) return parsed.response;
        const body = yield* bodyReader.read(request);
        if (!body.ok) return body.response;
        const isEmpty = body.byteLength === 0;
        if (isEmpty && !parsed.wantClose) return responses.badRequest("Empty body not allowed");
        if (!parsed.contentType && !isEmpty) return responses.badRequest("Content-Type required");
        // Malformed JSON is a request fault the edge must reject before framing.
        // An empty message set is a protocol rejection, so the rule lives in one place.
        const normalized = Create.normalizeInitialData(body.data, parsed.contentType ?? undefined);
        if (!normalized.ok) return normalized.response;
        const result = yield* writer.append(id, {
          data: body.data,
          contentType: parsed.contentType ?? "application/octet-stream",
          seq: parsed.seq,
          producer:
            parsed.producerHeaders.kind === "ok" ? parsed.producerHeaders.producer : undefined,
          close: parsed.wantClose,
          expectedOffset: parsed.expectedOffset,
        });
        return Append.toResponse(result, parsed.producerHeaders, isEmpty);
      }
      case "GET":
        return yield* read(reader, id, url, headers, cacheControl);
      case "HEAD": {
        const output = new Headers({
          "content-type": meta.contentType,
          "stream-next-offset": meta.nextOffset,
          "cache-control": "no-store",
        });
        if (meta.ttlSeconds) output.set("stream-ttl", String(meta.ttlSeconds));
        if (meta.expiresAt) output.set("stream-expires-at", meta.expiresAt);
        if (meta.closed) output.set("stream-closed", "true");
        return responses.empty(200, output);
      }
      case "DELETE": {
        yield* writer.remove(id);
        return responses.empty(204);
      }
      default:
        return responses.methodNotAllowed();
    }
  }).pipe(
    Effect.catchIf(isProtocolError, failure),
    Effect.catchTags({
      StorageFault: () => Effect.succeed(responses.internalError()),
      TransportFault: () => Effect.succeed(responses.internalError()),
    }),
    Effect.map((response) => {
      // Raw preserves the legacy Web response's byte and content-type conventions.
      const result =
        response instanceof Response
          ? HttpServerResponse.raw(response, {
              status: response.status,
              statusText: response.statusText,
              headers: Object.fromEntries(response.headers),
            })
          : response;
      return HttpServerResponse.setHeaders(result, {
        "x-content-type-options": "nosniff",
        "cross-origin-resource-policy": "cross-origin",
      });
    }),
  );
}
