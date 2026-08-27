import { Schema } from "effect";

const Recovery = Schema.Literal("snapshot-then-live");

/** Server-side schema for the browser-safe public error union. */
export const StateSinkPublicErrorSchema = Schema.Union([
  Schema.TaggedStruct("InvalidSinkParams", {
    sink: Schema.String,
    parameter: Schema.String,
    detail: Schema.String,
  }),
  Schema.TaggedStruct("ProtocolVersionUnsupported", {
    sink: Schema.String,
    supported: Schema.Finite,
    received: Schema.String,
    recovery: Recovery,
  }),
  Schema.TaggedStruct("ResumeRejected", {
    sink: Schema.String,
    reason: Schema.Literals([
      "invalid-offset",
      "history-unavailable",
      "protocol-incompatible",
      "contract-changed",
    ]),
    recovery: Recovery,
  }),
  Schema.TaggedStruct("SnapshotUnavailable", {
    sink: Schema.String,
    detail: Schema.String,
  }),
  Schema.TaggedStruct("TransportUnavailable", {
    sink: Schema.String,
    detail: Schema.String,
  }),
  Schema.TaggedStruct("WireDecodeFailed", {
    sink: Schema.String,
    detail: Schema.String,
  }),
]);
