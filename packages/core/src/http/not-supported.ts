/**
 * Mapping from structured `not-supported` protocol results to HTTP
 * responses. Used by handlers when a protocol method covering optional
 * behaviour returns `{ status: "not-supported", feature }`. Returns 400 Bad
 * Request with a body describing the unsupported feature and a
 * `stream-not-supported` header carrying the machine-readable feature id.
 */
import type { NotSupportedResult } from "../types/factory.ts";
import { isNotSupported } from "../types/factory.ts";
import { HttpResponseFactory } from "./responses.ts";

export function notSupportedResponse(
  result: NotSupportedResult,
  responses: HttpResponseFactory,
): Response {
  const detail = result.message ?? `Feature not supported: ${result.feature}`;
  return responses.text(detail, 400, { "stream-not-supported": result.feature });
}

/**
 * Convenience for use in `case`-style handlers that may want to short
 * circuit on a `not-supported` result before pattern-matching the rest of a
 * union.
 */
export function maybeNotSupportedResponse<T>(
  result: T | NotSupportedResult,
  responses: HttpResponseFactory,
): Response | null {
  return isNotSupported(result) ? notSupportedResponse(result, responses) : null;
}
