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
export function isErrnoException(cause: unknown): cause is NodeJS.ErrnoException {
  return (
    cause instanceof Error &&
    "code" in cause &&
    cause.code !== null &&
    cause.code !== undefined &&
    Object(cause.code) !== cause.code &&
    cause.code.constructor === String
  );
}

/** The complete value domain produced by parsing persisted JSON. */
export type PersistedJson =
  | null
  | boolean
  | number
  | string
  | PersistedJson[]
  | PersistedJsonObject;

/** A persisted JSON object with recursively validated JSON values. */
export interface PersistedJsonObject {
  readonly [key: string]: PersistedJson;
}

/**
 * Whether `value` is a plain JSON object — the only shape any file this adapter
 * persists may decode to. Arrays and `null` are excluded: both are objects to
 * `typeof`, and both would otherwise be indexed as records of named fields.
 */
export function isJsonObject(value: PersistedJson | undefined): value is PersistedJsonObject {
  return value !== null && Object(value) === value && !Array.isArray(value);
}

/** Whether a persisted property is a JSON string. */
export function isJsonString(value: PersistedJson | undefined): value is string {
  return (
    value !== null && value !== undefined && Object(value) !== value && value.constructor === String
  );
}

/** Whether a persisted property is a JSON number. */
export function isJsonNumber(value: PersistedJson | undefined): value is number {
  return (
    value !== null && value !== undefined && Object(value) !== value && value.constructor === Number
  );
}
