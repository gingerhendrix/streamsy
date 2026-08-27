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

const ClientFailureSchema = Schema.Struct({
  status: Schema.Literal("error"),
  code: ClientErrorCode,
  message: Schema.String,
  httpStatus: Schema.optional(Schema.Finite),
  retryable: Schema.Boolean,
  cause: Schema.optional(Schema.Unknown),
});

const isClientFailureSchema = Schema.is(ClientFailureSchema);

type ClientFailureSource = Schema.Schema.Type<ReturnType<typeof Schema.Defect>>;

const isClientFailure = (value: ClientFailureSource): value is ClientFailure =>
  isClientFailureSchema(value);

interface ClientFailureDetails {
  readonly code: ClientFailure["code"];
  readonly retryable: boolean;
  readonly message: string;
}

export class StreamCreateError extends Schema.TaggedError<StreamCreateError>()(
  "StreamCreateError",
  {
    operation: Schema.String,
    failure: Schema.Defect(),
    message: Schema.String,
    code: ClientErrorCode,
    retryable: Schema.Boolean,
    durability: Schema.Literal("unknown"),
  },
) {
  static from(operation: string, failure: ClientFailureSource): StreamCreateError {
    const details = decodeClientFailureDetails(failure);
    return new StreamCreateError({
      operation,
      failure,
      ...details,
      durability: "unknown",
    });
  }
}

export class StreamReadError extends Schema.TaggedError<StreamReadError>()("StreamReadError", {
  operation: Schema.String,
  failure: Schema.Defect(),
  message: Schema.String,
  code: ClientErrorCode,
  retryable: Schema.Boolean,
}) {
  static from(operation: string, failure: ClientFailureSource): StreamReadError {
    const details = decodeClientFailureDetails(failure);
    return new StreamReadError({
      operation,
      failure,
      ...details,
    });
  }
}

export class StreamAppendError extends Schema.TaggedError<StreamAppendError>()(
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
  static from(operation: string, failure: ClientFailureSource): StreamAppendError {
    const details = decodeClientFailureDetails(failure);
    return new StreamAppendError({
      operation,
      failure,
      ...details,
      durability: "unknown",
    });
  }
}

export class MalformedLineage extends Schema.TaggedError<MalformedLineage>()("MalformedLineage", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

export class IncompatibleLineage extends Schema.TaggedError<IncompatibleLineage>()(
  "IncompatibleLineage",
  { message: Schema.String },
) {}

export class MalformedSourceBoundary extends Schema.TaggedError<MalformedSourceBoundary>()(
  "MalformedSourceBoundary",
  { offset: Schema.String, cause: Schema.Defect() },
) {}

export class ProjectionPoison extends Schema.TaggedError<ProjectionPoison>()("ProjectionPoison", {
  phase: Schema.Literals(["decode", "reduce", "step", "membership", "member", "remove"]),
  sourcePosition: Schema.String,
  cause: Schema.Defect(),
}) {}

/** Durable target State could not be restored into typed application state. */
export class StateRestorePoison extends Schema.TaggedError<StateRestorePoison>()(
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

function decodeClientFailureDetails(failure: ClientFailureSource): ClientFailureDetails {
  if (isClientFailure(failure)) {
    return { code: failure.code, retryable: failure.retryable, message: failure.message };
  }
  try {
    return {
      code: "unknown",
      retryable: false,
      message: failure instanceof Error ? failure.message : String(failure),
    };
  } catch {
    return { code: "unknown", retryable: false, message: "Unknown client failure" };
  }
}
