import * as v from "valibot";

/** The Alchemy output members consumed by the conformance runner. */
export interface DeploymentOutput {
  readonly name: string;
  readonly url: string;
}

export const MAX_PREVIEW_WORKER_NAME_LENGTH = 54;
const WORKER_NAME_PREFIX = "streamsy-conf-server-";

const DeploymentState = v.object({
  output: v.object({
    name: v.pipe(v.string(), v.nonEmpty()),
    url: v.pipe(v.string(), v.nonEmpty()),
  }),
});

const normalizeNamePart = (value: string): string =>
  value.replaceAll(/[^a-z0-9_-]/gi, "-").toLowerCase();

/** Returns the physical Worker name Alchemy derives for this deployment stage. */
export function workerNameForStage(stage: string): string {
  return `${WORKER_NAME_PREFIX}${normalizeNamePart(stage)}`;
}

/**
 * Builds an isolated stage while reserving enough room for Alchemy's app and
 * resource prefixes under Cloudflare's preview-enabled script-name limit.
 */
export function uniqueConformanceStage(baseStage: string, runId: string): string {
  const stageBudget = MAX_PREVIEW_WORKER_NAME_LENGTH - WORKER_NAME_PREFIX.length;
  const suffix = normalizeNamePart(runId).slice(-12) || "run";
  const base = normalizeNamePart(baseStage) || "conformance";
  const baseBudget = Math.max(0, stageBudget - suffix.length - 1);
  return `${base.slice(0, baseBudget)}-${suffix}`.slice(-stageBudget);
}

/**
 * Parses Alchemy state once and returns the deployment output required by the
 * conformance runner. Syntax errors remain visible to the caller. A valid JSON
 * value with a malformed or missing output contract returns `null`.
 */
export function deploymentOutputFromJson(text: string): DeploymentOutput | null {
  const decoded = v.safeParse(DeploymentState, JSON.parse(text));
  return decoded.success ? decoded.output.output : null;
}

/** Converts a validated deployment output into the base worker URL. */
export function workerUrlFrom(output: DeploymentOutput): string {
  return output.url.replace(/\/$/, "");
}
