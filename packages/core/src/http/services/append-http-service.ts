import type { AppendResult } from "../../types/protocol.ts";
import type { BoundHttpRouteContext } from "../types.ts";
import { maybeNotSupportedResponse } from "../not-supported.ts";
import { ProducerHeaderParser, type ProducerHeaderResult } from "../producer-header-parser.ts";
import { RequestBodyReader } from "../request-body-reader.ts";
import { HttpResponseFactory } from "../responses.ts";

export class AppendHttpService {
  constructor(
    private deps: {
      responses: HttpResponseFactory;
      bodyReader: RequestBodyReader;
      producerHeaders: ProducerHeaderParser;
    },
  ) {}

  async execute(ctx: BoundHttpRouteContext): Promise<Response> {
    const parsed = this.parseHeaders(ctx.request);
    if (!parsed.ok) return parsed.response;
    const body = await this.readAndValidateBody(ctx.request, parsed.contentType, parsed.wantClose);
    if (!body.ok) return body.response;
    const result = await ctx.stream.append({
      data: body.data,
      contentType: parsed.contentType ?? "application/octet-stream",
      seq: parsed.seq,
      producer: parsed.producerHeaders.kind === "ok" ? parsed.producerHeaders.producer : undefined,
      close: parsed.wantClose,
    });
    if (result.status === "not-supported")
      return maybeNotSupportedResponse(result, this.deps.responses)!;
    return this.toResponse(result, parsed.producerHeaders, body.isEmpty);
  }

  private parseHeaders(request: Request):
    | {
        ok: true;
        contentType: string | null;
        seq?: string;
        wantClose: boolean;
        producerHeaders: ProducerHeaderResult;
      }
    | { ok: false; response: Response } {
    const producerHeaders = this.deps.producerHeaders.parse(request);
    if (producerHeaders.kind === "invalid")
      return { ok: false, response: this.deps.responses.badRequest("Invalid producer headers") };
    return {
      ok: true,
      contentType: request.headers.get("content-type"),
      seq: request.headers.get("stream-seq") ?? undefined,
      wantClose: request.headers.get("stream-closed")?.toLowerCase() === "true",
      producerHeaders,
    };
  }

  private async readAndValidateBody(
    request: Request,
    contentType: string | null,
    wantClose: boolean,
  ): Promise<{ ok: true; data: Uint8Array; isEmpty: boolean } | { ok: false; response: Response }> {
    const body = await this.deps.bodyReader.read(request);
    if (!body.ok) return body;
    const isEmpty = body.byteLength === 0;
    if (isEmpty && !wantClose)
      return { ok: false, response: this.deps.responses.badRequest("Empty body not allowed") };
    if (!contentType && !isEmpty)
      return { ok: false, response: this.deps.responses.badRequest("Content-Type required") };
    if (!isEmpty && contentType?.toLowerCase().startsWith("application/json")) {
      try {
        const parsed = JSON.parse(new TextDecoder().decode(body.data));
        if (Array.isArray(parsed) && parsed.length === 0)
          return {
            ok: false,
            response: this.deps.responses.badRequest("Empty arrays not allowed"),
          };
      } catch (error) {
        if (error instanceof SyntaxError)
          return { ok: false, response: this.deps.responses.invalidJson() };
        throw error;
      }
    }
    return { ok: true, data: body.data, isEmpty };
  }

  private toResponse(
    result: Exclude<AppendResult, { status: "not-supported" }>,
    producerHeaders: ProducerHeaderResult,
    isEmpty: boolean,
  ): Response {
    switch (result.status) {
      case "not-found":
        return this.deps.responses.notFound();
      case "gone":
        return this.deps.responses.gone();
      case "conflict": {
        if (result.conflictReason === "closed") {
          return this.deps.responses.empty(409, {
            "stream-closed": "true",
            "stream-next-offset": result.nextOffset,
          });
        }
        return this.deps.responses.conflict(
          result.conflictReason === "content-type" ? "Content-Type mismatch" : "Sequence conflict",
        );
      }
      case "stale-epoch":
        return this.deps.responses.text("Stale producer epoch", 403, {
          "producer-epoch": String(result.currentEpoch),
        });
      case "producer-gap":
        return this.deps.responses.conflict("Producer sequence gap", {
          "producer-expected-seq": String(result.expectedSeq),
          "producer-received-seq": String(result.receivedSeq),
        });
      case "invalid-epoch-seq":
        return this.deps.responses.badRequest("New epoch must start at seq=0");
      case "duplicate":
        return this.deps.responses.empty(204, {
          "stream-next-offset": result.nextOffset,
          "producer-epoch": String(result.producerEpoch),
          "producer-seq": String(result.producerSeq),
          ...(result.closed ? { "stream-closed": "true" } : {}),
        });
      case "appended": {
        const headers: Record<string, string> = {
          "stream-next-offset": result.nextOffset,
          ...(result.closed ? { "stream-closed": "true" } : {}),
        };
        if (result.producerEpoch !== undefined)
          headers["producer-epoch"] = String(result.producerEpoch);
        if (result.producerSeq !== undefined) headers["producer-seq"] = String(result.producerSeq);
        return this.deps.responses.empty(
          producerHeaders.kind === "ok" && !isEmpty ? 200 : 204,
          headers,
        );
      }
    }
  }
}
