import { Schema } from "effect";
import { HttpServerResponse } from "effect/http";

export class YjsProtocolError extends Schema.TaggedError<
  YjsProtocolError,
  { readonly brand: unique symbol }
>()("YjsProtocolError", {
  code: Schema.String,
  message: Schema.String,
  status: Schema.Int,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** The Yjs wire error envelope is shared by the entry and document routes. */
export const protocolErrorResponse = ({ code, message, status }: YjsProtocolError) =>
  HttpServerResponse.jsonUnsafe({ error: { code, message } }, { status });

export const unavailable = () =>
  new YjsProtocolError({
    code: "INTERNAL_ERROR",
    message: "Document unavailable; retry",
    status: 503,
  });
