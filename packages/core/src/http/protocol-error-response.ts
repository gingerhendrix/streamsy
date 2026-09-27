import { Match } from "effect";
import type { ProtocolError } from "../protocol/errors.ts";
import * as Responses from "./responses.ts";
const responses = Responses;

/** The sole protocol rejection mapping at the HTTP edge. */
export function protocolErrorResponse(
  error: ProtocolError,
  context: { readonly method: string },
): Response {
  const conflict = ({ message }: { readonly message: string }) => responses.conflict(message);
  const badRequest = ({ message }: { readonly message: string }) => responses.badRequest(message);
  return Match.valueTags(error, {
    StreamNotFound: () =>
      context.method === "HEAD" ? responses.noStore(responses.notFound()) : responses.notFound(),
    StreamGone: () =>
      context.method === "HEAD" ? responses.noStore(responses.gone()) : responses.gone(),
    // The body names the source, which is the observable symptom of a fork that
    // landed on an owner that does not hold its source stream.
    ForkSourceNotFound: ({ source }) => responses.notFound(`Source stream not found: ${source}`),
    CreateConflict: conflict,
    AppendConflict: conflict,
    InvalidReadRequest: badRequest,
    InvalidForkRequest: badRequest,
    InvalidAppendRequest: badRequest,
    StreamClosed: ({ offset }) =>
      responses.empty(409, { "stream-closed": "true", "stream-next-offset": offset }),
    OffsetMismatch: ({ actual }) =>
      responses.conflict("Expected offset mismatch", { "stream-next-offset": actual }),
    StreamBusy: () => responses.text("Stream busy, retry later", 503),
    StaleEpoch: ({ currentEpoch }) =>
      responses.text("Stale producer epoch", 403, { "producer-epoch": String(currentEpoch) }),
    ProducerGap: ({ expectedSeq, receivedSeq }) =>
      responses.conflict("Producer sequence gap", {
        "producer-expected-seq": String(expectedSeq),
        "producer-received-seq": String(receivedSeq),
      }),
    InvalidEpochSeq: () => responses.badRequest("New epoch must start at seq=0"),
    NotSupported: ({ feature }) =>
      new Response(`Feature not supported: ${feature}`, {
        status: 400,
        headers: { "stream-not-supported": feature },
      }),
  });
}
