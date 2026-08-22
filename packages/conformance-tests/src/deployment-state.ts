/**
 * Reading the deployed worker's URL out of Alchemy's state file.
 *
 * The state file is untrusted input to the conformance run: Alchemy writes it,
 * a previous or partial deploy may have left anything behind, and its
 * `output.url` decides which server the whole suite is then pointed at. Trusting
 * `JSON.parse` to have produced the expected shape made a state file with no
 * `output`, a non-object `output`, or a non-string `url` indistinguishable from
 * a good one until something downstream failed. These checks reject every such
 * value up front, at the one place the URL enters the run.
 *
 * Parsing lives here, apart from `deploy-test-destroy.ts`, because that script
 * deploys on import and so cannot be exercised by a test.
 */

/** Whether `value` is a plain JSON object — the only shape the state file may take. */
function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * The deployed worker's URL, or `null` for any state value that does not carry
 * one. Every malformed shape returns `null` rather than a distinct error, so the
 * caller reports one message naming the file it read.
 */
export function workerUrlFrom(state: unknown): string | null {
  if (!isJsonObject(state)) return null;
  const output = state.output;
  if (!isJsonObject(output)) return null;
  const url = output.url;
  if (typeof url !== "string" || url.length === 0) return null;
  return url.replace(/\/$/, "");
}
