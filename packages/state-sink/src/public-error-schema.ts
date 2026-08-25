import { Schema } from "effect";

const Recovery = Schema.Literal("snapshot-then-live");

/** Server-side schema for the browser-safe public error union. */
export const StateSinkPublicErrorSchema = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("InvalidSinkParams"),
    sink: Schema.String,
    parameter: Schema.String,
    detail: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ProtocolVersionUnsupported"),
    sink: Schema.String,
    supported: Schema.Number,
    received: Schema.String,
    recovery: Recovery,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ResumeRejected"),
    sink: Schema.String,
    reason: Schema.Literals([
      "invalid-offset",
      "history-unavailable",
      "protocol-incompatible",
      "contract-changed",
    ]),
    recovery: Recovery,
  }),
  Schema.Struct({
    _tag: Schema.Literal("SnapshotUnavailable"),
    sink: Schema.String,
    detail: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("TransportUnavailable"),
    sink: Schema.String,
    detail: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("WireDecodeFailed"),
    sink: Schema.String,
    detail: Schema.String,
  }),
]);
