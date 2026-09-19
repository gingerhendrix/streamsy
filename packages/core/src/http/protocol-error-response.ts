import type { ProtocolError } from "../protocol/errors.ts";
import * as Responses from "./responses.ts";
const responses = Responses;

/** The sole protocol rejection mapping at the HTTP edge. */
export function protocolErrorResponse(
  error: ProtocolError,
  context: { readonly method: string },
): Response {
  switch (error._tag) {
    case "StreamNotFound":
      return context.method === "HEAD"
        ? responses.noStore(responses.notFound())
        : responses.notFound();
    case "StreamGone":
      return context.method === "HEAD" ? responses.noStore(responses.gone()) : responses.gone();
    case "ForkSourceNotFound":
      // The body names the source, which is the observable symptom of a fork that
      // landed on an owner that does not hold its source stream.
      return responses.notFound(`Source stream not found: ${error.source}`);
    case "CreateConflict":
    case "AppendConflict":
      return responses.conflict(error.message);
    case "InvalidReadRequest":
    case "InvalidForkRequest":
    case "InvalidAppendRequest":
      return responses.badRequest(error.message);
    case "StreamClosed":
      return responses.empty(409, { "stream-closed": "true", "stream-next-offset": error.offset });
    case "OffsetMismatch":
      return responses.conflict("Expected offset mismatch", { "stream-next-offset": error.actual });
    case "StreamBusy":
      return responses.text("Stream busy, retry later", 503);
    case "StaleEpoch":
      return responses.text("Stale producer epoch", 403, {
        "producer-epoch": String(error.currentEpoch),
      });
    case "ProducerGap":
      return responses.conflict("Producer sequence gap", {
        "producer-expected-seq": String(error.expectedSeq),
        "producer-received-seq": String(error.receivedSeq),
      });
    case "InvalidEpochSeq":
      return responses.badRequest("New epoch must start at seq=0");
    case "NotSupported":
      return new Response(`Feature not supported: ${error.feature}`, {
        status: 400,
        headers: { "stream-not-supported": error.feature },
      });
  }
}
