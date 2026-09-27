import { Schema } from "effect";
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
  ValueUnavailable: { route: Schema.String, detail: Schema.String },
  WireEncodeFailed: { route: Schema.String, detail: Schema.String },
}) satisfies Schema.Codec<PublicError>;
