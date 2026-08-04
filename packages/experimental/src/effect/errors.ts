import type { ClientFailure } from "@streamsy/core";
import { Schema } from "effect";

const ClientErrorCode = Schema.Literals([
  "transport",
  "unauthorized",
  "forbidden",
  "rate-limited",
  "bad-request",
  "busy",
  "parse-error",
  "not-supported",
  "aborted",
  "client-closed",
  "unknown",
]);

export class StreamReadError extends Schema.TaggedErrorClass<StreamReadError>()("StreamReadError", {
  operation: Schema.String,
  failure: Schema.Defect(),
  message: Schema.String,
  code: ClientErrorCode,
  retryable: Schema.Boolean,
}) {
  static from(operation: string, failure: ClientFailure | unknown): StreamReadError {
    const classification = clientFailureClassification(failure);
    return new StreamReadError({
      operation,
      failure,
      message: isClientFailure(failure)
        ? failure.message
        : failure instanceof Error
          ? failure.message
          : String(failure),
      ...classification,
    });
  }
}

export class StreamAppendError extends Schema.TaggedErrorClass<StreamAppendError>()(
  "StreamAppendError",
  {
    operation: Schema.String,
    failure: Schema.Defect(),
    message: Schema.String,
    code: ClientErrorCode,
    retryable: Schema.Boolean,
    durability: Schema.Literal("unknown"),
  },
) {
  static from(operation: string, failure: ClientFailure | unknown): StreamAppendError {
    const classification = clientFailureClassification(failure);
    return new StreamAppendError({
      operation,
      failure,
      message: isClientFailure(failure)
        ? failure.message
        : failure instanceof Error
          ? failure.message
          : String(failure),
      ...classification,
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

function clientFailureClassification(failure: ClientFailure | unknown) {
  return isClientFailure(failure)
    ? { code: failure.code, retryable: failure.retryable }
    : { code: "unknown" as const, retryable: false };
}

function isClientFailure(value: unknown): value is ClientFailure {
  return (
    typeof value === "object" && value !== null && "status" in value && value.status === "error"
  );
}
