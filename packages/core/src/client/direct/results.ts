/**
 * Residual protocol-result -> client-result narrowing for the direct adapter.
 *
 * The direct adapter is nearly a pass-through: the internal protocol already
 * speaks result objects, so this module only narrows the members the common
 * API promises and folds the rest into the single {@link ClientFailure}.
 */

import type { AppendResult, CreateResult } from "../../types/protocol.ts";
import { isNotSupportedError } from "../../types/storage-adapter.ts";
import type {
  ClientAppendResult,
  ClientCloseResult,
  ClientCreateResult,
  ClientErrorCode,
  ClientFailure,
} from "../types.ts";

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

/** Maps an unexpected thrown value (not a protocol result) to a failure. */
export function failureFromThrown(error: unknown): ClientFailure {
  if (error instanceof SyntaxError) {
    return failure("parse-error", error.message, { cause: error });
  }
  if (isNotSupportedError(error)) {
    return failure("not-supported", error.message, { cause: error });
  }
  return failure("unknown", "Stream operation failed", { cause: error });
}

/** Narrows the shared absent/failure statuses of a get/metadata/read result. */
export function absentResult(result: {
  status: string;
  feature?: string;
}): { status: "not-found" } | { status: "gone" } | ClientFailure {
  if (result.status === "not-found") return { status: "not-found" };
  if (result.status === "gone") return { status: "gone" };
  return residualFailure(result);
}

export function mapCreate(result: CreateResult): ClientCreateResult {
  if (result.status === "created") return { status: "created", contentType: result.contentType };
  if (result.status === "exists" || result.status === "conflict") return { status: "conflict" };
  return residualFailure(result);
}

export function mapAppend(result: AppendResult): ClientAppendResult {
  if (result.status === "appended" || result.status === "duplicate") return { status: "appended" };
  if (result.status === "not-found") return { status: "not-found" };
  if (result.status === "gone") return { status: "gone" };
  if (result.status === "conflict") {
    return result.conflictReason === "closed" ? { status: "closed" } : { status: "conflict" };
  }
  return residualFailure(result);
}

export function mapClose(result: AppendResult): ClientCloseResult {
  if (result.status === "appended" || result.status === "duplicate") {
    return { status: "closed", finalOffset: result.offset };
  }
  if (result.status === "not-found") return { status: "not-found" };
  if (result.status === "gone") return { status: "gone" };
  if (result.status === "conflict") return { status: "conflict" };
  return residualFailure(result);
}

/** Maps a mid-read terminal protocol result to a read-session failure. */
export function readFailure(result: { status: string; feature?: string }): ClientFailure {
  if (result.status === "not-found") return failure("unknown", "Stream not found during read");
  if (result.status === "gone") return failure("unknown", "Stream is gone during read");
  return residualFailure(result);
}

function residualFailure(result: { status: string; feature?: string }): ClientFailure {
  switch (result.status) {
    case "not-supported":
      return failure("not-supported", `Feature is not supported: ${result.feature}`);
    case "bad-request":
    case "invalid-epoch-seq":
      return failure("bad-request", "Invalid stream request");
    case "busy":
      return failure("busy", "Stream is busy", { retryable: true });
    default:
      return failure("unknown", `Unexpected stream result: ${result.status}`);
  }
}
