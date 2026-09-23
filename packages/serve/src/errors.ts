import { Schema } from "effect";
import type { PublicError } from "./contract.ts";

export const PublicErrorSchema: Schema.Codec<PublicError> = Schema.Union([
  Schema.TaggedStruct("InvalidParams", {
    route: Schema.String,
    parameter: Schema.String,
    detail: Schema.String,
  }),
  Schema.TaggedStruct("ProtocolVersionUnsupported", {
    route: Schema.String,
    supported: Schema.Literal(1),
    received: Schema.String,
    recovery: Schema.Literal("replay-from-start"),
  }),
  Schema.TaggedStruct("ResumeRejected", {
    route: Schema.String,
    reason: Schema.Literals(["invalid-offset", "history-unavailable", "contract-changed"]),
    recovery: Schema.Literal("replay-from-start"),
  }),
  Schema.TaggedStruct("ContractChanged", {
    route: Schema.String,
    recovery: Schema.Literal("refetch"),
  }),
  Schema.TaggedStruct("TransportUnavailable", { route: Schema.String, detail: Schema.String }),
  Schema.TaggedStruct("DocumentUnavailable", { route: Schema.String, detail: Schema.String }),
  Schema.TaggedStruct("WireEncodeFailed", { route: Schema.String, detail: Schema.String }),
]);
