import type { AppendResult } from "../protocol/results.ts";
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
  result: AppendResult,
  producerHeaders: ProducerHeaderResult,
  isEmpty: boolean,
): Response {
  switch (result._tag) {
    case "Duplicate":
      const duplicateHeaders = new Headers({
        "stream-next-offset": result.offset,
        "producer-epoch": String(result.producerEpoch),
        "producer-seq": String(result.producerSeq),
      });
      if (result.closed) duplicateHeaders.set("stream-closed", "true");
      return responses.empty(204, duplicateHeaders);
    case "Appended": {
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
