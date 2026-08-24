import * as v from "valibot";

/** The Alchemy output members consumed by the conformance runner. */
export interface DeploymentOutput {
  readonly url: string;
}

const DeploymentState = v.object({
  output: v.object({
    url: v.pipe(v.string(), v.nonEmpty()),
  }),
});

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
