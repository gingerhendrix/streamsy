import { Effect, Schema } from "effect";
import { HttpServerRespondable, HttpServerResponse } from "effect/http";
import { Http } from "@streamsy/core";
import type { PublicError } from "./contract.ts";

/** The public error body. `cases` gives one validating constructor per tag. */
export const PublicErrorSchema = Schema.TaggedUnion({
  InvalidParams: {
    route: Schema.String,
    parameter: Schema.String,
    detail: Schema.String,
  },
  ProtocolVersionUnsupported: {
    route: Schema.String,
    supported: Schema.Literal(1),
    received: Schema.String,
    recovery: Schema.Literal("replay-from-start"),
  },
  ResumeRejected: {
    route: Schema.String,
    reason: Schema.Literals(["invalid-offset", "history-unavailable", "contract-changed"]),
    recovery: Schema.Literal("replay-from-start"),
  },
  ContractChanged: {
    route: Schema.String,
    recovery: Schema.Literal("refetch"),
  },
  TransportUnavailable: { route: Schema.String, detail: Schema.String },
  WireEncodeFailed: { route: Schema.String, detail: Schema.String },
}) satisfies Schema.Codec<PublicError>;

export class InvalidParams extends Schema.TaggedError<InvalidParams>()(
  "InvalidParams",
  PublicErrorSchema.cases.InvalidParams.fields,
  { httpApiStatus: 400 },
) {
  responseHeaders: Readonly<Record<string, string>> = {};
  [HttpServerRespondable.symbol]() {
    return Effect.succeed(errorResponse(this, 400, this.responseHeaders));
  }
}

export class ProtocolVersionUnsupported extends Schema.TaggedError<ProtocolVersionUnsupported>()(
  "ProtocolVersionUnsupported",
  PublicErrorSchema.cases.ProtocolVersionUnsupported.fields,
  { httpApiStatus: 409 },
) {
  responseHeaders: Readonly<Record<string, string>> = {};
  [HttpServerRespondable.symbol]() {
    return Effect.succeed(errorResponse(this, 409, this.responseHeaders));
  }
}

export class ResumeRejected extends Schema.TaggedError<ResumeRejected>()(
  "ResumeRejected",
  PublicErrorSchema.cases.ResumeRejected.fields,
  { httpApiStatus: 409 },
) {
  responseHeaders: Readonly<Record<string, string>> = {};
  [HttpServerRespondable.symbol]() {
    return Effect.succeed(errorResponse(this, 409, this.responseHeaders));
  }
}

export class ContractChanged extends Schema.TaggedError<ContractChanged>()(
  "ContractChanged",
  PublicErrorSchema.cases.ContractChanged.fields,
  { httpApiStatus: 409 },
) {
  responseHeaders: Readonly<Record<string, string>> = {};
  [HttpServerRespondable.symbol]() {
    return Effect.succeed(errorResponse(this, 409, this.responseHeaders));
  }
}

export class TransportUnavailable extends Schema.TaggedError<TransportUnavailable>()(
  "TransportUnavailable",
  PublicErrorSchema.cases.TransportUnavailable.fields,
  { httpApiStatus: 503 },
) {
  responseHeaders: Readonly<Record<string, string>> = {};
  [HttpServerRespondable.symbol]() {
    return Effect.succeed(errorResponse(this, 503, this.responseHeaders));
  }
}

export class WireEncodeFailed extends Schema.TaggedError<WireEncodeFailed>()(
  "WireEncodeFailed",
  PublicErrorSchema.cases.WireEncodeFailed.fields,
  { httpApiStatus: 500 },
) {
  responseHeaders: Readonly<Record<string, string>> = {};
  [HttpServerRespondable.symbol]() {
    return Effect.succeed(errorResponse(this, 500, this.responseHeaders));
  }
}

export type ServeError =
  | InvalidParams
  | ProtocolVersionUnsupported
  | ResumeRejected
  | ContractChanged
  | TransportUnavailable
  | WireEncodeFailed;

/** Response metadata stays out of the public JSON body. */
export function withHeaders<E extends ServeError>(
  error: E,
  headers: Readonly<Record<string, string>>,
): E {
  error.responseHeaders = headers;
  return error;
}
function errorResponse(
  error: PublicError,
  status: number,
  headers: Readonly<Record<string, string>>,
) {
  return HttpServerResponse.jsonUnsafe(Schema.encodeSync(PublicErrorSchema)(error), {
    status,
    headers: { ...Http.securityHeaders, ...headers, "cache-control": "no-store" },
  });
}
