import { Schema } from "effect";

export class DeriveFault extends Schema.TaggedError<DeriveFault>()("DeriveFault", {
  reason: Schema.Literals([
    "history-unavailable",
    "identity-mismatch",
    "invalid-state",
    "sink-conflict",
    "storage-failure",
    "unsupported-composition",
    "invalid-source",
    "invalid-limits",
  ]),
  message: Schema.String,
}) {}
