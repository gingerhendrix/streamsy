import type { AppendOutcome } from "../protocol/outcomes.ts";
import { isValid } from "../offset/index.ts";
import { ProducerHeaderParser, type ProducerHeaderResult } from "./producer-header-parser.ts";
const producerParser = new ProducerHeaderParser();
import { HttpResponseFactory } from "./responses.ts";
const responses = new HttpResponseFactory();
export function parseHeaders(request: { readonly headers: Headers }):
  | {
      ok: true;
      contentType: string | null;
      seq?: string;
      wantClose: boolean;
      expectedOffset?: string;
      producerHeaders: ProducerHeaderResult;
    }
  | { ok: false; response: Response } {
  const producerHeaders = producerParser.parse(request);
  if (producerHeaders.kind === "invalid")
    return { ok: false, response: responses.badRequest("Invalid producer headers") };
  // Streamsy extension: optimistic-concurrency precondition (see docs/api.md).
  const expectedOffset = request.headers.get("stream-expected-offset") ?? undefined;
  if (expectedOffset !== undefined && !isValid(expectedOffset))
    return { ok: false, response: responses.badRequest("Invalid expected offset") };
  return {
    ok: true,
    contentType: request.headers.get("content-type"),
    seq: request.headers.get("stream-seq") ?? undefined,
    wantClose: request.headers.get("stream-closed")?.toLowerCase() === "true",
    expectedOffset,
    producerHeaders,
  };
}

export function toResponse(
  result: Exclude<AppendOutcome, { status: "not-supported" }>,
  producerHeaders: ProducerHeaderResult,
  isEmpty: boolean,
): Response {
  switch (result.status) {
    case "not-found":
      return responses.notFound();
    case "gone":
      return responses.gone();
    case "conflict": {
      if (result.conflictReason === "closed") {
        return responses.empty(409, {
          "stream-closed": "true",
          "stream-next-offset": result.offset,
        });
      }
      if (result.conflictReason === "expected-offset") {
        return responses.conflict("Expected offset mismatch", {
          "stream-next-offset": result.offset,
        });
      }
      return responses.conflict(
        result.conflictReason === "content-type" ? "Content-Type mismatch" : "Sequence conflict",
      );
    }
    case "busy":
      return responses.text("Stream busy, retry later", 503);
    case "stale-epoch":
      return responses.text("Stale producer epoch", 403, {
        "producer-epoch": String(result.currentEpoch),
      });
    case "producer-gap":
      return responses.conflict("Producer sequence gap", {
        "producer-expected-seq": String(result.expectedSeq),
        "producer-received-seq": String(result.receivedSeq),
      });
    case "invalid-epoch-seq":
      return responses.badRequest("New epoch must start at seq=0");
    case "duplicate":
      const duplicateHeaders = new Headers({
        "stream-next-offset": result.offset,
        "producer-epoch": String(result.producerEpoch),
        "producer-seq": String(result.producerSeq),
      });
      if (result.closed) duplicateHeaders.set("stream-closed", "true");
      return responses.empty(204, duplicateHeaders);
    case "appended": {
      const headers = new Headers({
        "stream-next-offset": result.offset,
      });
      if (result.closed) headers.set("stream-closed", "true");
      if (result.producerEpoch !== undefined)
        headers.set("producer-epoch", String(result.producerEpoch));
      if (result.producerSeq !== undefined) headers.set("producer-seq", String(result.producerSeq));
      return responses.empty(producerHeaders.kind === "ok" && !isEmpty ? 200 : 204, headers);
    }
  }

  return exhaustive(result);
}

function exhaustive(value: never): never {
  throw new TypeError(`Unexpected result: ${String(value)}`);
}
