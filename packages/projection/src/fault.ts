import { Schema } from "effect";
import { isStreamsFault } from "@streamsy/core";

/** Phase says where the run stopped; a `checkpoint` failure means processing already completed. */
export class ProjectionFault extends Schema.TaggedError<ProjectionFault>()("ProjectionFault", {
  phase: Schema.Literals(["load", "read", "pin", "process", "checkpoint"]),
  reason: Schema.Literals([
    "history-unavailable",
    "identity-mismatch",
    "invalid-record",
    "invalid-source",
    "invalid-options",
    "invalid-output",
    "token-conflict",
    "range-unreproducible",
    "stale-epoch",
    "storage-failure",
    "unsupported-composition",
  ]),
  input: Schema.optionalKey(Schema.String),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  get retryable(): boolean {
    return (
      this.reason === "storage-failure" && (!isStreamsFault(this.cause) || this.cause.retryable)
    );
  }
}
