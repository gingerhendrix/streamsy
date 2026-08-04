import type { ClientFailure } from "@streamsy/core";
import { Schema } from "effect";

export class StreamReadError extends Schema.TaggedErrorClass<StreamReadError>()(
  "StreamReadError",
  {
    operation: Schema.String,
    failure: Schema.Defect(),
    message: Schema.String,
  },
) {
  static from(operation: string, failure: ClientFailure | unknown): StreamReadError {
    return new StreamReadError({
      operation,
      failure,
      message:
        isClientFailure(failure) ? failure.message : failure instanceof Error ? failure.message : String(failure),
    });
  }
}

export class StreamAppendError extends Schema.TaggedErrorClass<StreamAppendError>()(
  "StreamAppendError",
  {
    operation: Schema.String,
    failure: Schema.Defect(),
    message: Schema.String,
    durability: Schema.Literal("unknown"),
  },
) {
  static from(operation: string, failure: ClientFailure | unknown): StreamAppendError {
    return new StreamAppendError({
      operation,
      failure,
      message:
        isClientFailure(failure) ? failure.message : failure instanceof Error ? failure.message : String(failure),
      durability: "unknown",
    });
  }
}

export class MalformedLineage extends Schema.TaggedErrorClass<MalformedLineage>()(
  "MalformedLineage",
  { message: Schema.String, cause: Schema.Defect() },
) {}

export class IncompatibleLineage extends Schema.TaggedErrorClass<IncompatibleLineage>()(
  "IncompatibleLineage",
  { message: Schema.String },
) {}

export class MalformedSourceBoundary extends Schema.TaggedErrorClass<MalformedSourceBoundary>()(
  "MalformedSourceBoundary",
  { offset: Schema.String, cause: Schema.Defect() },
) {}

export class ProjectionPoison extends Schema.TaggedErrorClass<ProjectionPoison>()(
  "ProjectionPoison",
  {
    phase: Schema.Literals(["decode", "reduce"]),
    sourcePosition: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export type MeshOperationalError =
  | StreamReadError
  | StreamAppendError
  | MalformedLineage
  | IncompatibleLineage
  | MalformedSourceBoundary
  | ProjectionPoison;

function isClientFailure(value: unknown): value is ClientFailure {
  return typeof value === "object" && value !== null && "status" in value && value.status === "error";
}
