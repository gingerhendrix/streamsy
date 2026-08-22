/**
 * Runtime narrowing for the two kinds of untrusted value this adapter handles:
 * values thrown by `node:fs`, and values parsed out of a persisted file.
 *
 * Both arrive as `unknown` and both were previously asserted into shape. An
 * assertion is a claim, not a check: it makes a corrupt file or an unexpected
 * throw indistinguishable from a valid one at the point it matters most — the
 * branch that decides whether a stream is missing, corrupt, or intact. These
 * guards do the check the assertions only claimed.
 */

/**
 * Whether `error` is a Node errno error whose `code` is safe to read.
 *
 * The adapter branches on `code` (`ENOENT`, `EEXIST`, `EPERM`) to tell an
 * expected filesystem state from a real failure. Only an `Error` carrying a
 * string `code` qualifies — the shape `node:fs` and `process.kill` raise.
 * Anything else (a `SyntaxError` from a corrupt file, a validation error raised
 * by this package, a thrown non-error) is not an errno error and must propagate.
 */
export function isErrnoException(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && "code" in error && typeof error.code === "string";
}

/**
 * Whether `value` is a plain JSON object — the only shape any file this adapter
 * persists may decode to. Arrays and `null` are excluded: both are objects to
 * `typeof`, and both would otherwise be indexed as records of named fields.
 */
export function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
