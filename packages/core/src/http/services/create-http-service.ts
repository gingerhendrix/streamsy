import type { StreamProtocolFactory } from "../../types/protocol.ts";
import type { HttpRouteContext } from "../types.ts";
import { maybeNotSupportedResponse } from "../not-supported.ts";
import { RequestBodyReader } from "../request-body-reader.ts";
import { HttpResponseFactory } from "../responses.ts";
import { StreamPathService } from "../stream-path-service.ts";

export class CreateHttpService {
  constructor(
    private deps: {
      protocol: StreamProtocolFactory;
      path: StreamPathService;
      responses: HttpResponseFactory;
      bodyReader: RequestBodyReader;
    },
  ) {}

  async execute(ctx: HttpRouteContext): Promise<Response> {
    const parsed = this.parseHeaders(ctx.request);
    if (!parsed.ok) return parsed.response;
    const body = await this.deps.bodyReader.read(ctx.request);
    if (!body.ok) return body.response;
    const normalized = this.normalizeInitialData(body.data, parsed.contentType);
    if (!normalized.ok) return normalized.response;
    const result = await this.deps.protocol.create(ctx.streamId, {
      contentType: parsed.contentType,
      ttlSeconds: parsed.ttlSeconds,
      expiresAt: parsed.expiresAt,
      initialData: normalized.initialData,
      closed: parsed.wantClosed,
      forkedFrom: parsed.forkedFromStreamId,
      forkOffset: parsed.forkOffset,
      forkSubOffset: parsed.forkSubOffset,
    });
    if (result.status === "not-supported")
      return maybeNotSupportedResponse(result, this.deps.responses)!;
    return this.toResponse(result, ctx.request.url);
  }

  private parseHeaders(request: Request):
    | {
        ok: true;
        contentType?: string;
        ttlSeconds?: number;
        expiresAt?: string;
        wantClosed: boolean;
        forkedFromStreamId?: string;
        forkOffset?: string;
        forkSubOffset?: number;
      }
    | { ok: false; response: Response } {
    const rawContentType = request.headers.get("content-type");
    const ttlHeader = request.headers.get("stream-ttl");
    const expiresAtHeader = request.headers.get("stream-expires-at");
    const forkedFromHeader = request.headers.get("stream-forked-from");
    const forkOffsetHeader = request.headers.get("stream-fork-offset");
    const forkSubOffsetHeader = request.headers.get("stream-fork-sub-offset");
    if (ttlHeader && !/^(0|[1-9]\d*)$/.test(ttlHeader))
      return { ok: false, response: this.deps.responses.badRequest("Invalid Stream-TTL format") };
    if (ttlHeader && expiresAtHeader)
      return {
        ok: false,
        response: this.deps.responses.badRequest(
          "Cannot specify both Stream-TTL and Stream-Expires-At",
        ),
      };
    if (expiresAtHeader && isNaN(new Date(expiresAtHeader).getTime()))
      return {
        ok: false,
        response: this.deps.responses.badRequest("Invalid Stream-Expires-At format"),
      };
    if (forkOffsetHeader && !forkedFromHeader)
      return {
        ok: false,
        response: this.deps.responses.badRequest("Stream-Fork-Offset requires Stream-Forked-From"),
      };
    if (forkSubOffsetHeader !== null) {
      if (!forkedFromHeader)
        return {
          ok: false,
          response: this.deps.responses.badRequest(
            "Stream-Fork-Sub-Offset requires Stream-Forked-From",
          ),
        };
      if (!/^(0|[1-9]\d*)$/.test(forkSubOffsetHeader))
        return {
          ok: false,
          response: this.deps.responses.badRequest("Invalid Stream-Fork-Sub-Offset format"),
        };
      if (parseInt(forkSubOffsetHeader, 10) > 0 && !forkOffsetHeader)
        return {
          ok: false,
          response: this.deps.responses.badRequest(
            "Stream-Fork-Sub-Offset greater than zero requires Stream-Fork-Offset",
          ),
        };
    }
    const isFork = !!forkedFromHeader;
    return {
      ok: true,
      contentType: rawContentType ?? (isFork ? undefined : "application/octet-stream"),
      ttlSeconds: ttlHeader ? parseInt(ttlHeader, 10) : undefined,
      expiresAt: expiresAtHeader ?? undefined,
      wantClosed: request.headers.get("stream-closed")?.toLowerCase() === "true",
      forkedFromStreamId: forkedFromHeader
        ? this.deps.path.canonicalizeForkSource(forkedFromHeader)
        : undefined,
      forkOffset: forkOffsetHeader ?? undefined,
      forkSubOffset: forkSubOffsetHeader !== null ? parseInt(forkSubOffsetHeader, 10) : undefined,
    };
  }

  private normalizeInitialData(
    data: Uint8Array,
    contentType?: string,
  ): { ok: true; initialData?: Uint8Array } | { ok: false; response: Response } {
    let effectiveInitialData: Uint8Array | undefined = data.byteLength > 0 ? data : undefined;
    if (effectiveInitialData && contentType?.toLowerCase().startsWith("application/json")) {
      try {
        const parsed = JSON.parse(new TextDecoder().decode(effectiveInitialData));
        if (Array.isArray(parsed) && parsed.length === 0) effectiveInitialData = undefined;
      } catch (error) {
        if (error instanceof SyntaxError)
          return { ok: false, response: this.deps.responses.invalidJson() };
        throw error;
      }
    }
    return { ok: true, initialData: effectiveInitialData };
  }

  private toResponse(
    result: Exclude<
      Awaited<ReturnType<StreamProtocolFactory["create"]>>,
      { status: "not-supported" }
    >,
    location: string,
  ): Response {
    if (result.status === "not-found")
      return this.deps.responses.notFound(result.errorMessage ?? "Source stream not found");
    if (result.status === "bad-request")
      return this.deps.responses.badRequest(result.errorMessage ?? "Invalid fork parameters");
    if (result.status === "conflict")
      return this.deps.responses.conflict(
        result.errorMessage ?? "Stream exists with different configuration",
      );
    const success = result as Extract<typeof result, { status: "created" | "exists" }>;
    const status = success.status === "created" ? 201 : 200;
    return this.deps.responses.empty(status, {
      "content-type": success.contentType,
      "stream-next-offset": success.nextOffset,
      ...(status === 201 ? { location } : {}),
      ...(success.closed ? { "stream-closed": "true" } : {}),
    });
  }
}
