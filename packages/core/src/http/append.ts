import { AppendResult } from "../protocol/results.ts";
import { isValid } from "../offset/index.ts";
import * as ProducerHeaders from "./producer-header-parser.ts";
import type { ProducerHeaderResult } from "./producer-header-parser.ts";
const producerParser = ProducerHeaders;
import * as Responses from "./responses.ts";
const responses = Responses;
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
  // Streamsy extension: optimistic-concurrency precondition.
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
  return AppendResult.$match(result, {
    Duplicate: (duplicate) => {
      const headers = new Headers({
        "stream-next-offset": duplicate.offset,
        "producer-epoch": String(duplicate.producerEpoch),
        "producer-seq": String(duplicate.producerSeq),
      });
      if (duplicate.closed) headers.set("stream-closed", "true");
      return responses.empty(204, headers);
    },
    Appended: (appended) => {
      const headers = new Headers({
        "stream-next-offset": appended.offset,
      });
      if (appended.closed) headers.set("stream-closed", "true");
      if (appended.producerEpoch !== undefined)
        headers.set("producer-epoch", String(appended.producerEpoch));
      if (appended.producerSeq !== undefined)
        headers.set("producer-seq", String(appended.producerSeq));
      return responses.empty(producerHeaders.kind === "ok" && !isEmpty ? 200 : 204, headers);
    },
  });
}
