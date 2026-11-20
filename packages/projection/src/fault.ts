import { Schema } from "effect";
import { isStreamsFault } from "@streamsy/core";

/** A typed handler failure bearing this marker stops its binding without retry. */
export const NonRetryable = Symbol.for("@streamsy/projection/NonRetryable");

/** No live runner for this declaration exists in the selected registry. */
export class NotAttached extends Schema.TaggedError<NotAttached>()("NotAttached", {
  projection: Schema.String,
}) {}

/** Encoded params do not round-trip through the attached declaration's codecs. */
export class InvalidBinding extends Schema.TaggedError<InvalidBinding>()("InvalidBinding", {
  projection: Schema.String,
  params: Schema.Record(Schema.String, Schema.String),
}) {}

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
    "output-busy",
    "range-unreproducible",
    "stale-epoch",
    "retired-generation",
    "storage-failure",
    "unsupported-composition",
  ]),
  input: Schema.optionalKey(Schema.String),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  get retryable(): boolean {
    switch (this.reason) {
      case "storage-failure":
        return !isStreamsFault(this.cause) || this.cause.retryable;
      case "token-conflict":
      case "output-busy":
        return true;
      case "history-unavailable":
      case "identity-mismatch":
      case "invalid-record":
      case "invalid-source":
      case "invalid-options":
      case "invalid-output":
      case "range-unreproducible":
      case "stale-epoch":
      case "retired-generation":
      case "unsupported-composition":
        return false;
    }

    return false;
  }
}
