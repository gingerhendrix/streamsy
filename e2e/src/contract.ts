import { Schema } from "effect";
import type { LatencyMeasurement, ThroughputMeasurement } from "./measurement.ts";

export const STACK_NAME = "streamsy-conf";
export const COMPATIBILITY_DATE = "2026-07-30";
export const COMPATIBILITY_FLAGS = ["nodejs_compat"] as const;
export const CONFORMANCE_LONG_POLL_TIMEOUT_MS = 1_500;
export const HOSTED_STATUS =
  "Alchemy deployment for Step 3 is authorized. Hosted execution remains disabled in this local package pending independently reviewed live adapters and a reconciled run plan, including destroy/cleanup and required query scope. Hosted acceptance still requires hosted evidence, the uploaded-compressed-byte/startup-CPU policy, and Gareth's budget/topology decision. The accepted Batch B local signal is 81,574 B gzip against the unchanged 27,160 B proposal. The 542.85 ms first-object p95 proposal remains unmeasured.";

export class ContractError extends Schema.TaggedError<ContractError>()("ContractError", {
  message: Schema.String,
}) {}

export const StackOutput = Schema.Struct({
  workerName: Schema.NonEmptyString,
  url: Schema.String,
});
export type StackOutput = typeof StackOutput.Type;

export const isValidStage = (stage: string): boolean =>
  stage.length <= 16 && /^c-[0-9a-z]+-[0-9a-f]{6}$/.test(stage);

export const makeStage = (timestampSeconds: number, runId: string): string => {
  if (!Number.isSafeInteger(timestampSeconds) || timestampSeconds < 0) {
    throw new ContractError({ message: "Stage timestamp must be a nonnegative safe integer" });
  }
  if (!/^[0-9a-f]{6}$/.test(runId)) {
    throw new ContractError({ message: "Stage run id must be six lowercase hex characters" });
  }
  const stage = `c-${timestampSeconds.toString(36)}-${runId}`;
  if (!isValidStage(stage))
    throw new ContractError({ message: "Stage is longer than 16 characters" });
  return stage;
};

export const expectedWorkerName = (stage: string): string => `${STACK_NAME}-${stage}`;

export type TargetDecode =
  | { readonly ok: true; readonly value: { readonly workerName: string; readonly url: string } }
  | { readonly ok: false; readonly reason: string };

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Stack output is an untrusted JSON boundary decoded immediately by StackOutput.
export const decodeWorkerTarget = (value: unknown, expectedName: string): TargetDecode => {
  try {
    const decoded = Schema.decodeUnknownSync(StackOutput)(value);
    const url = new URL(decoded.url);
    if (decoded.workerName !== expectedName) {
      return { ok: false, reason: "Stack output worker name did not match the planned worker" };
    }
    if (
      url.protocol !== "https:" ||
      url.username !== "" ||
      url.password !== "" ||
      url.search !== "" ||
      url.hash !== "" ||
      url.pathname !== "/" ||
      url.port !== "" ||
      !url.hostname.endsWith(".workers.dev")
    ) {
      return { ok: false, reason: "Stack output URL was not a normalized workers.dev URL" };
    }
    return { ok: true, value: { workerName: decoded.workerName, url: `${url.origin}/` } };
  } catch {
    return { ok: false, reason: "Stack output was missing or malformed" };
  }
};

export type StepStatus = "pending" | "success" | "failure" | "unavailable";
export type StepResult<T> =
  | { readonly status: "pending" }
  | { readonly status: "success"; readonly value: T }
  | { readonly status: "failure"; readonly reason: string; readonly details?: T }
  | { readonly status: "unavailable"; readonly reason: string };

export const pending = <T>(): StepResult<T> => ({ status: "pending" });
export const success = <T>(value: T): StepResult<T> => ({ status: "success", value });
export const failure = <T>(reason: string, details?: T): StepResult<T> =>
  details === undefined ? { status: "failure", reason } : { status: "failure", reason, details };
export const unavailable = <T>(reason: string): StepResult<T> => ({
  status: "unavailable",
  reason,
});

export interface RunIdentity {
  readonly runId: string;
  readonly stage: string;
  readonly workerName: string;
  readonly cwd: string;
  readonly sourceSha: string;
  readonly artifactSha256: string;
}

export interface MetadataAvailability {
  readonly status: "available" | "unavailable";
  readonly value?: number;
  readonly provenance: string;
  readonly reason?: string;
}

export interface EvidenceReport extends RunIdentity {
  readonly url?: string;
  readonly readiness: StepResult<{ readonly attempts: number }>;
  readonly metadata: StepResult<{
    readonly reportedScriptSize: MetadataAvailability;
    readonly downloadedModuleBytes: MetadataAvailability;
    readonly actualUploadedCompressedBytes: MetadataAvailability;
    readonly startupCpuMs: MetadataAvailability;
  }>;
  readonly conformance: StepResult<{
    readonly passed: number;
    readonly skipped: number;
    readonly status: string;
  }>;
  readonly measurement: StepResult<MeasurementEvidence>;
  readonly destroyFirst: StepResult<{ readonly code: number }>;
  readonly destroySecond: StepResult<{ readonly code: number }>;
  readonly gone: StepResult<{ readonly status: number }>;
  readonly audit: StepResult<{
    readonly complete: boolean;
    readonly workers: ReadonlyArray<string>;
  }>;
  readonly primaryFailure?: string;
  readonly cleanupFailures: ReadonlyArray<string>;
  readonly reportWriteFailure?: string;
  readonly hostedStatus: string;
}

export interface MeasurementEvidence {
  readonly latency: LatencyMeasurement;
  readonly throughput: ThroughputMeasurement;
}
