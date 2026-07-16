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
  error: unknown,
  signal?: AbortSignal,
): OfficialClassification {
  if (signal?.aborted || isAbortError(error) || error instanceof FetchBackoffAbortError) {
    return { kind: "aborted", failure: abortedFailure(error) };
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
    failure: failure("unknown", "Stream operation failed", { cause: error }),
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

export function headErrorResult(error: unknown, signal?: AbortSignal): ClientHeadResult {
  const classified = classifyOfficialError(error, signal);
  if (classified.kind === "not-found") return { status: "not-found" };
  if (classified.kind === "gone") return { status: "gone" };
  return classified.failure;
}

export function createErrorResult(error: unknown, signal?: AbortSignal): ClientCreateResult {
  const classified = classifyOfficialError(error, signal);
  return classified.kind === "conflict" ? { status: "conflict" } : classified.failure;
}

export function appendErrorResult(error: unknown, signal?: AbortSignal): ClientAppendResult {
  const classified = classifyOfficialError(error, signal);
  switch (classified.kind) {
    case "not-found":
      return { status: "not-found" };
    case "gone":
      return { status: "gone" };
    case "closed":
      return { status: "closed" };
    case "conflict":
      return { status: "conflict" };
    default:
      return classified.failure;
  }
}

export function closeErrorResult(error: unknown, signal?: AbortSignal): ClientCloseResult {
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
  error: unknown,
  signal?: AbortSignal,
): Exclude<ClientReadResult, { status: "ok" }> {
  const classified = classifyOfficialError(error, signal);
  if (classified.kind === "not-found") return { status: "not-found" };
  if (classified.kind === "gone") return { status: "gone" };
  return classified.failure;
}

export function readEndFailure(error: unknown, signal?: AbortSignal): ClientFailure {
  return classifyOfficialError(error, signal).failure;
}

function isRetryable(code: ClientErrorCode): boolean {
  return code === "transport" || code === "busy" || code === "rate-limited";
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}
