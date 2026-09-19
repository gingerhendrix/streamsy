import { DateTime, Option } from "effect";
import type { CreateResult } from "../protocol/results.ts";
import { streamPath } from "./stream-path-service.ts";
import * as Responses from "./responses.ts";
const responses = Responses;
export function parseHeaders(
  request: { readonly headers: Headers },
  path: ReturnType<typeof streamPath>,
):
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
    return { ok: false, response: responses.badRequest("Invalid Stream-TTL format") };
  if (ttlHeader && expiresAtHeader)
    return {
      ok: false,
      response: responses.badRequest("Cannot specify both Stream-TTL and Stream-Expires-At"),
    };
  if (expiresAtHeader && Option.isNone(DateTime.make(expiresAtHeader)))
    return {
      ok: false,
      response: responses.badRequest("Invalid Stream-Expires-At format"),
    };
  if (forkOffsetHeader && !forkedFromHeader)
    return {
      ok: false,
      response: responses.badRequest("Stream-Fork-Offset requires Stream-Forked-From"),
    };
  if (forkSubOffsetHeader !== null) {
    if (!forkedFromHeader)
      return {
        ok: false,
        response: responses.badRequest("Stream-Fork-Sub-Offset requires Stream-Forked-From"),
      };
    if (!/^(0|[1-9]\d*)$/.test(forkSubOffsetHeader))
      return {
        ok: false,
        response: responses.badRequest("Invalid Stream-Fork-Sub-Offset format"),
      };
    if (parseInt(forkSubOffsetHeader, 10) > 0 && !forkOffsetHeader)
      return {
        ok: false,
        response: responses.badRequest(
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
      ? path.canonicalizeForkSource(forkedFromHeader)
      : undefined,
    forkOffset: forkOffsetHeader ?? undefined,
    forkSubOffset: forkSubOffsetHeader !== null ? parseInt(forkSubOffsetHeader, 10) : undefined,
  };
}

export function normalizeInitialData(
  data: Uint8Array,
  contentType?: string,
): { ok: true; initialData?: Uint8Array } | { ok: false; response: Response } {
  let effectiveInitialData: Uint8Array | undefined = data.byteLength > 0 ? data : undefined;
  if (effectiveInitialData && contentType?.toLowerCase().startsWith("application/json")) {
    try {
      const parsed = JSON.parse(new TextDecoder().decode(effectiveInitialData));
      if (Array.isArray(parsed) && parsed.length === 0) effectiveInitialData = undefined;
    } catch (error) {
      if (error instanceof SyntaxError) return { ok: false, response: responses.invalidJson() };
      throw error;
    }
  }
  return { ok: true, initialData: effectiveInitialData };
}

export function toResponse(result: CreateResult, location: string): Response {
  switch (result._tag) {
    case "Created":
    case "Exists": {
      const status = result._tag === "Created" ? 201 : 200;
      const headers = new Headers({
        "content-type": result.contentType,
        "stream-next-offset": result.nextOffset,
      });
      if (status === 201) headers.set("location", location);
      if (result.closed) headers.set("stream-closed", "true");
      return responses.empty(status, headers);
    }
  }

  return exhaustive(result);
}

function exhaustive(value: never): never {
  throw new TypeError(`Unexpected result: ${String(value)}`);
}
