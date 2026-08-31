import {
  DurableStreamError,
  FetchBackoffAbortError,
  FetchError,
  StreamClosedError,
} from "@durable-streams/client";
import type {
  ClientAppendResult,
  ClientCloseResult,
  ClientCreateResult,
  ClientErrorCode,
  ClientFailure,
  ClientHeadResult,
  ClientReadResult,
} from "@streamsy/core";

/**
 * Classifies an error thrown by the official `@durable-streams/client` into a
 * domain kind (which each operation maps to its own result member) plus a
 * fallback {@link ClientFailure} for the cross-cutting cases.
 *
 * This is the official adapter's only real error work: mapping the official
 * thrown-error taxonomy into the transport-neutral result vocabulary once.
 */
export interface OfficialClassification {
  kind: "aborted" | "not-found" | "gone" | "conflict" | "closed" | "other";
  failure: ClientFailure;
}

interface UnrecognizedOfficialError {
  readonly kind: "unrecognized-official-error";
  readonly cause: unknown;
}

export type OfficialError =
  | DurableStreamError
  | FetchBackoffAbortError
  | FetchError
  | StreamClosedError
  | TypeError
  | DOMException
  | UnrecognizedOfficialError;

/** Decodes a thrown value once before operation-specific error mapping. */
export function decodeOfficialError(cause: unknown): OfficialError {
  if (
    cause instanceof DurableStreamError ||
    cause instanceof FetchBackoffAbortError ||
    cause instanceof FetchError ||
    cause instanceof StreamClosedError ||
    cause instanceof TypeError ||
    cause instanceof DOMException
  ) {
    return cause;
  }
  return { kind: "unrecognized-official-error", cause };
}

export function failure(
  code: ClientErrorCode,
  message: string,
  options: { retryable?: boolean; httpStatus?: number; cause?: unknown } = {},
): ClientFailure {
  return {
    status: "error",
    code,
    message,
    httpStatus: options.httpStatus,
    retryable: options.retryable ?? false,
    cause: options.cause,
  };
}

export function abortedFailure(cause?: unknown): ClientFailure {
  return failure("aborted", "Stream operation was aborted", { cause });
}

export function clientClosedFailure(): ClientFailure {
  return failure("client-closed", "Stream protocol client is closed");
}

export function classifyOfficialError(
  error: OfficialError,
  signal?: AbortSignal,
): OfficialClassification {
  const cause =
    "kind" in error && error.kind === "unrecognized-official-error" ? error.cause : error;
  if (signal?.aborted || isAbortError(error) || error instanceof FetchBackoffAbortError) {
    return { kind: "aborted", failure: abortedFailure(cause) };
  }
  if (error instanceof StreamClosedError) {
    return {
      kind: "closed",
      failure: failure("bad-request", error.message, { httpStatus: error.status, cause: error }),
    };
  }
  if (error instanceof DurableStreamError) {
    return classifyStatusAndCode(error.status, error.code, error.message, error);
  }
  if (error instanceof FetchError) {
    const closed = error.headers["stream-closed"]?.toLowerCase() === "true";
    if (error.status === 409 && closed) {
      return {
        kind: "closed",
        failure: failure("bad-request", error.message, { httpStatus: error.status, cause: error }),
      };
    }
    return classifyStatusAndCode(error.status, undefined, error.message, error);
  }
  if (error instanceof TypeError) {
    return {
      kind: "other",
      failure: failure("transport", error.message, { retryable: true, cause: error }),
    };
  }
  return {
    kind: "other",
    failure: failure("unknown", "Stream operation failed", { cause }),
  };
}

function classifyStatusAndCode(
  status: number | undefined,
  code: DurableStreamError["code"] | undefined,
  message: string,
  cause: unknown,
): OfficialClassification {
  const domain = domainFromStatus(status);
  if (domain)
    return { kind: domain, failure: failure("unknown", message, { httpStatus: status, cause }) };
  const failureCode = failureFromStatus(status) ?? (code ? failureFromCode(code) : undefined);
  if (failureCode === "closed") {
    return {
      kind: "closed",
      failure: failure("bad-request", message, { httpStatus: status, cause }),
    };
  }
  if (failureCode === "not-found")
    return { kind: "not-found", failure: failure("unknown", message, { cause }) };
  if (failureCode === "conflict")
    return { kind: "conflict", failure: failure("unknown", message, { cause }) };
  const resolved = failureCode ?? "unknown";
  return {
    kind: "other",
    failure: failure(resolved, message, {
      httpStatus: status,
      retryable: isRetryable(resolved),
      cause,
    }),
  };
}

function domainFromStatus(status?: number): OfficialClassification["kind"] | undefined {
  switch (status) {
    case 404:
      return "not-found";
    case 409:
      return "conflict";
    case 410:
      return "gone";
    default:
      return undefined;
  }
}

function failureFromStatus(status?: number): ClientErrorCode | undefined {
  switch (status) {
    case 400:
      return "bad-request";
    case 401:
      return "unauthorized";
    case 403:
      return "forbidden";
    case 429:
      return "rate-limited";
    case 503:
      return "busy";
    default:
      return undefined;
  }
}

type ExtendedCode = ClientErrorCode | "closed" | "not-found" | "conflict";

function failureFromCode(code: DurableStreamError["code"]): ExtendedCode {
  switch (code) {
    case "NOT_FOUND":
      return "not-found";
    case "CONFLICT_SEQ":
    case "CONFLICT_EXISTS":
    case "ALREADY_CONSUMED":
      return "conflict";
    case "ALREADY_CLOSED":
    case "STREAM_CLOSED":
      return "closed";
    case "BAD_REQUEST":
      return "bad-request";
    case "BUSY":
      return "busy";
    case "SSE_NOT_SUPPORTED":
      return "not-supported";
    case "UNAUTHORIZED":
      return "unauthorized";
    case "FORBIDDEN":
      return "forbidden";
    case "RATE_LIMITED":
      return "rate-limited";
    case "PARSE_ERROR":
      return "parse-error";
    default:
      return "unknown";
  }
}

export function headErrorResult(error: OfficialError, signal?: AbortSignal): ClientHeadResult {
  const classified = classifyOfficialError(error, signal);
  if (classified.kind === "not-found") return { status: "not-found" };
  if (classified.kind === "gone") return { status: "gone" };
  return classified.failure;
}

export function createErrorResult(error: OfficialError, signal?: AbortSignal): ClientCreateResult {
  const classified = classifyOfficialError(error, signal);
  return classified.kind === "conflict" ? { status: "conflict" } : classified.failure;
}

export function appendErrorResult(error: OfficialError, signal?: AbortSignal): ClientAppendResult {
  if (error instanceof FetchError) {
    const rich = richAppendError(error);
    if (rich) return rich;
  }
  const classified = classifyOfficialError(error, signal);
  switch (classified.kind) {
    case "not-found":
      return { status: "not-found" };
    case "gone":
      return { status: "gone" };
    case "closed":
      return failure("parse-error", "Closed append response omitted Stream-Next-Offset", {
        httpStatus: classified.failure.httpStatus,
        cause: error,
      });
    case "conflict":
      return { status: "conflict", conflictReason: "sequence" };
    default:
      return classified.failure;
  }
}

function richAppendError(error: FetchError): ClientAppendResult | undefined {
  const header = (name: string) => error.headers[name.toLowerCase()];
  if (error.status === 409 && header("stream-closed")?.toLowerCase() === "true") {
    const offset = header("stream-next-offset");
    return offset
      ? { status: "closed", offset }
      : failure("parse-error", "Closed append response omitted Stream-Next-Offset", {
          httpStatus: error.status,
          cause: error,
        });
  }
  if (error.status === 409 && header("producer-expected-seq") !== undefined) {
    const expectedSeq = safeIntegerHeader(header("producer-expected-seq"));
    const receivedSeq = safeIntegerHeader(header("producer-received-seq"));
    if (expectedSeq === undefined || receivedSeq === undefined) {
      return failure("parse-error", "Producer gap response contained invalid sequence headers", {
        httpStatus: error.status,
        cause: error,
      });
    }
    return { status: "producer-gap", expectedSeq, receivedSeq };
  }
  if (error.status === 409 && error.text === "Expected offset mismatch") {
    const offset = header("stream-next-offset");
    return offset
      ? { status: "conflict", conflictReason: "expected-offset", offset }
      : failure("parse-error", "Expected-offset conflict omitted Stream-Next-Offset", {
          httpStatus: error.status,
          cause: error,
        });
  }
  if (error.status === 409 && error.text === "Content-Type mismatch") {
    return { status: "conflict", conflictReason: "content-type" };
  }
  if (error.status === 409) return { status: "conflict", conflictReason: "sequence" };
  if (error.status === 403 && header("producer-epoch") !== undefined) {
    const currentEpoch = safeIntegerHeader(header("producer-epoch"));
    return currentEpoch === undefined
      ? failure("parse-error", "Stale-epoch response contained an invalid Producer-Epoch", {
          httpStatus: error.status,
          cause: error,
        })
      : { status: "stale-epoch", currentEpoch };
  }
  if (error.status === 400 && error.text === "New epoch must start at seq=0") {
    return { status: "invalid-epoch-seq" };
  }
  return undefined;
}

function safeIntegerHeader(value: string | undefined): number | undefined {
  if (value === undefined || !/^(0|[1-9]\d*)$/.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

export function closeErrorResult(error: OfficialError, signal?: AbortSignal): ClientCloseResult {
  const classified = classifyOfficialError(error, signal);
  switch (classified.kind) {
    case "not-found":
      return { status: "not-found" };
    case "gone":
      return { status: "gone" };
    case "conflict":
    case "closed":
      return { status: "conflict" };
    default:
      return classified.failure;
  }
}

/** Returns only the T-independent members, so it composes with any `ClientReadResult<T>`. */
export function readErrorResult(
  error: OfficialError,
  signal?: AbortSignal,
): Exclude<ClientReadResult, { status: "ok" }> {
  const classified = classifyOfficialError(error, signal);
  if (classified.kind === "not-found") return { status: "not-found" };
  if (classified.kind === "gone") return { status: "gone" };
  return classified.failure;
}

export function readEndFailure(error: OfficialError, signal?: AbortSignal): ClientFailure {
  return classifyOfficialError(error, signal).failure;
}

function isRetryable(code: ClientErrorCode): boolean {
  return code === "transport" || code === "busy" || code === "rate-limited";
}

function isAbortError(error: OfficialError): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}
