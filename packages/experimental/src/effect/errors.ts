import type { ClientFailure } from "@streamsy/core";
import { Schema } from "effect";

const CLIENT_ERROR_CODES = [
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
] as const;

const ClientErrorCode = Schema.Literals(CLIENT_ERROR_CODES);

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
      message: clientFailureMessage(failure),
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
      message: clientFailureMessage(failure),
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
    phase: Schema.Literals(["decode", "reduce", "step", "membership", "member", "remove"]),
    sourcePosition: Schema.String,
    cause: Schema.Defect(),
  },
) {}

/** Durable target State could not be restored into typed application state. */
export class StateRestorePoison extends Schema.TaggedErrorClass<StateRestorePoison>()(
  "StateRestorePoison",
  { targetOffset: Schema.String, cause: Schema.Defect() },
) {}

export type MeshOperationalError =
  | StreamReadError
  | StreamAppendError
  | MalformedLineage
  | IncompatibleLineage
  | MalformedSourceBoundary
  | ProjectionPoison
  | StateRestorePoison;

function clientFailureClassification(failure: ClientFailure | unknown) {
  return isClientFailure(failure)
    ? { code: failure.code, retryable: failure.retryable }
    : { code: "unknown" as const, retryable: false };
}

function isClientFailure(value: unknown): value is ClientFailure {
  try {
    return (
      typeof value === "object" &&
      value !== null &&
      "status" in value &&
      value.status === "error" &&
      "code" in value &&
      typeof value.code === "string" &&
      CLIENT_ERROR_CODES.some((code) => code === value.code) &&
      "message" in value &&
      typeof value.message === "string" &&
      "retryable" in value &&
      typeof value.retryable === "boolean"
    );
  } catch {
    return false;
  }
}

function clientFailureMessage(failure: unknown): string {
  if (isClientFailure(failure)) return failure.message;
  try {
    if (failure instanceof Error && typeof failure.message === "string") return failure.message;
    return String(failure);
  } catch {
    return "Unknown client failure";
  }
}
