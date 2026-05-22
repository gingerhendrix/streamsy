/**
 * Content type normalization and matching for the durable streams protocol.
 *
 * Responsibilities:
 *
 * - `normalizeContentType` lowercases, strips RFC 7231 parameters (everything
 *   after the first `;`), and trims surrounding whitespace.
 * - `contentTypeMatches` compares two content types by equality of their
 *   normalized forms.
 *
 * Used by `AppendService` for append content-type conflicts, by
 * `CreateStreamService` via `configMatches` for idempotent create checks, and
 * by `ForkService` for fork content-type compatibility.
 */

export function normalizeContentType(ct: string): string {
  return ct.toLowerCase().split(";")[0]!.trim();
}

export function contentTypeMatches(expected: string, actual: string): boolean {
  return normalizeContentType(expected) === normalizeContentType(actual);
}
