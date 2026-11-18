import { Schema } from "effect";
import { HttpServerResponse } from "effect/http";

export class YjsProtocolError extends Schema.TaggedError<
  YjsProtocolError,
  { readonly brand: unique symbol }
>()("YjsProtocolError", {
  code: Schema.String,
  message: Schema.String,
  status: Schema.Int,
}) {}

/** The Yjs wire error envelope. */
export const protocolErrorResponse = ({ code, message, status }: YjsProtocolError) =>
  HttpServerResponse.jsonUnsafe({ error: { code, message } }, { status });

/** A tagged failure. Streamsy faults also say whether a retry can succeed. */
interface Failure {
  readonly _tag: string;
  readonly retryable?: boolean;
}

/** A retryable fault is a `503`; any other unexpected failure is a `500`. */
export const unexpected = ({ retryable }: Failure) =>
  retryable === true
    ? new YjsProtocolError({
        code: "INTERNAL_ERROR",
        message: "Document unavailable; retry",
        status: 503,
      })
    : new YjsProtocolError({ code: "INTERNAL_ERROR", message: "Internal error", status: 500 });
