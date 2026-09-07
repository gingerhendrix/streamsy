/* oxlint-disable typescript/consistent-return -- Effect failure branches use return-yield* for never-successful effects; successful probes return their value. */
import { Cause, Context, Effect, Exit, Ref, Schedule } from "effect";
import {
  ContractError,
  HOSTED_STATUS,
  type EvidenceReport,
  type MeasurementEvidence,
  type MetadataAvailability,
  type RunIdentity,
  decodeWorkerTarget,
  expectedWorkerName,
  failure,
  isValidStage,
  pending,
  success,
  unavailable,
} from "./contract.ts";
import {
  HttpOperation,
  measureLatency,
  measureThroughput,
  type FullResponse,
  type LatencyOptions,
  type ThroughputOptions,
} from "./measurement.ts";

export interface DeployInput extends RunIdentity {}
export interface DestroyInput extends RunIdentity {
  readonly url?: string;
}
export interface ConformanceResult {
  readonly passed: number;
  readonly skipped: number;
  readonly status: string;
}
export interface MetadataResult {
  readonly reportedScriptSize: MetadataAvailability;
  readonly downloadedModuleBytes: MetadataAvailability;
  readonly actualUploadedCompressedBytes: MetadataAvailability;
  readonly startupCpuMs: MetadataAvailability;
}
export interface AuditResult {
  readonly complete: boolean;
  readonly workers: ReadonlyArray<string>;
}

export interface EvidenceMeasurementOptions {
  readonly latency: LatencyOptions;
  readonly throughput: ThroughputOptions;
}

export interface EvidenceRunOptions {
  readonly measurement?: EvidenceMeasurementOptions;
}

export interface EvidenceOperationsService {
  readonly deploy: (input: DeployInput) => Effect.Effect<unknown, ContractError>;
  readonly destroy: (
    input: DestroyInput,
  ) => Effect.Effect<{ readonly code: number }, ContractError>;
  readonly runOfficialSuite: (
    input: RunIdentity & { readonly url: string },
  ) => Effect.Effect<ConformanceResult, ContractError>;
  readonly captureMetadata: (
    input: RunIdentity & { readonly url: string },
  ) => Effect.Effect<MetadataResult, ContractError>;
  readonly auditExactWorker: (input: RunIdentity) => Effect.Effect<AuditResult, ContractError>;
}

export class EvidenceOperations extends Context.Service<
  EvidenceOperations,
  EvidenceOperationsService
>()("@streamsy/hosted/EvidenceOperations") {}

export interface ReportPersistenceService {
  readonly write: (report: EvidenceReport) => Effect.Effect<void, ContractError>;
}

export class ReportPersistence extends Context.Service<
  ReportPersistence,
  ReportPersistenceService
>()("@streamsy/hosted/ReportPersistence") {}

const readinessBody = (response: FullResponse): boolean =>
  response.status === 400 && response.body === "Stream path required: /{path}";

const boundedPoll = (
  url: string,
  expected: (response: FullResponse) => boolean,
  label: string,
): Effect.Effect<{ readonly attempts: number }, ContractError, HttpOperation> =>
  Effect.gen(function* () {
    const http = yield* HttpOperation;
    const consecutive = yield* Ref.make(0);
    const attempts = yield* Ref.make(0);
    const pass = Effect.gen(function* () {
      yield* Ref.update(attempts, (count) => count + 1);
      const response = yield* Effect.exit(
        http.request({ method: "GET", url }).pipe(Effect.timeout("5 seconds")),
      );
      if (Exit.isSuccess(response) && expected(response.value)) {
        const count = yield* Ref.updateAndGet(consecutive, (value) => value + 1);
        if (count >= 5) return;
      } else {
        yield* Ref.set(consecutive, 0);
      }
      return yield* Effect.fail(
        new ContractError({ message: `${label} probe did not satisfy its marker` }),
      );
    });
    const retryPolicy = Schedule.spaced("2 seconds").pipe(Schedule.upTo({ times: 30 }));
    yield* pass.pipe(
      Effect.retry({ schedule: retryPolicy }),
      Effect.timeout("60 seconds"),
      Effect.catch(() => Effect.fail(new ContractError({ message: `${label} probe timed out` }))),
    );
    return { attempts: yield* Ref.get(attempts) };
  });

export const awaitReady = (
  url: string,
): Effect.Effect<{ readonly attempts: number }, ContractError, HttpOperation> =>
  boundedPoll(url, readinessBody, "Readiness");

export const awaitGone = (
  url: string,
): Effect.Effect<{ readonly attempts: number }, ContractError, HttpOperation> =>
  boundedPoll(url, (response) => response.status === 404, "Gone");

const initialReport = (identity: RunIdentity): EvidenceReport => ({
  ...identity,
  readiness: pending(),
  metadata: pending(),
  conformance: pending(),
  measurement: unavailable<MeasurementEvidence>("No fake measurement configuration was supplied"),
  destroyFirst: pending(),
  destroySecond: pending(),
  gone: pending(),
  audit: pending(),
  cleanupFailures: [],
  hostedStatus: HOSTED_STATUS,
});

interface State {
  readonly report: EvidenceReport;
  readonly started: boolean;
  readonly url?: string;
}

interface ExitStep {
  readonly result: EvidenceReport["destroyFirst"];
  readonly error?: string;
}

const errorMessage = (cause: Cause.Cause<unknown>): string => Cause.pretty(cause);

const captureEffect = <A, R>(
  thunk: () => Effect.Effect<A, ContractError, R>,
): Effect.Effect<Exit.Exit<A, ContractError>, never, R> => Effect.exit(Effect.suspend(thunk));

const exitStep = (
  exit: Exit.Exit<{ readonly code: number }, ContractError>,
  label: string,
): ExitStep => {
  if (Exit.isSuccess(exit) && exit.value.code === 0) return { result: success(exit.value) };
  if (Exit.isSuccess(exit)) {
    return {
      result: failure(`${label} returned non-zero code ${exit.value.code}`, exit.value),
      error: `${label} returned non-zero code ${exit.value.code}`,
    };
  }
  return {
    result: failure(`${label}: ${errorMessage(exit.cause)}`),
    error: `${label}: ${errorMessage(exit.cause)}`,
  };
};

const cleanup = (state: State): Effect.Effect<State, never, EvidenceOperations | HttpOperation> =>
  Effect.gen(function* () {
    const operations = yield* EvidenceOperations;
    const nextFailures: Array<string> = [...state.report.cleanupFailures];
    const destroyInput: DestroyInput = { ...state.report, url: state.url };
    const first = yield* captureEffect(() => operations.destroy(destroyInput));
    const firstStep = exitStep(first, "destroy #1");
    if (firstStep.error !== undefined) nextFailures.push(firstStep.error);
    const second = yield* captureEffect(() => operations.destroy(destroyInput));
    const secondStep = exitStep(second, "destroy #2");
    if (secondStep.error !== undefined) nextFailures.push(secondStep.error);
    let gone = unavailable<{ readonly status: number }>("Validated worker URL was unavailable");
    const url = state.url;
    if (url !== undefined) {
      const goneExit = yield* captureEffect(() => awaitGone(url));
      if (Exit.isSuccess(goneExit)) gone = success({ status: 404 });
      else {
        gone = failure(`gone: ${errorMessage(goneExit.cause)}`);
        nextFailures.push(`gone: ${errorMessage(goneExit.cause)}`);
      }
    }
    const auditExit = yield* captureEffect(() => operations.auditExactWorker(state.report));
    let audit = unavailable<AuditResult>("Exact-worker audit was unavailable");
    if (Exit.isSuccess(auditExit)) {
      if (!auditExit.value.complete) {
        audit = failure("Exact-worker audit was incomplete", auditExit.value);
        nextFailures.push("audit: incomplete authenticated pagination");
      } else if (auditExit.value.workers.includes(state.report.workerName)) {
        audit = failure("Exact-worker audit found the planned worker", auditExit.value);
        nextFailures.push("audit: planned worker still present");
      } else {
        audit = success(auditExit.value);
      }
    } else {
      audit = failure(`audit: ${errorMessage(auditExit.cause)}`);
      nextFailures.push(`audit: ${errorMessage(auditExit.cause)}`);
    }
    return {
      ...state,
      report: {
        ...state.report,
        destroyFirst: firstStep.result,
        destroySecond: secondStep.result,
        gone,
        audit,
        cleanupFailures: nextFailures,
      },
    };
  });

const validateIdentity = (identity: RunIdentity): void => {
  if (!isValidStage(identity.stage)) throw new ContractError({ message: "Run stage was invalid" });
  if (identity.workerName !== expectedWorkerName(identity.stage)) {
    throw new ContractError({ message: "Run worker name did not match the stage" });
  }
  if (
    identity.cwd.length === 0 ||
    identity.sourceSha.length === 0 ||
    identity.artifactSha256.length === 0
  ) {
    throw new ContractError({ message: "Run identity was incomplete" });
  }
};

export const runEvidence = (
  identity: RunIdentity,
  options: EvidenceRunOptions = {},
): Effect.Effect<void, ContractError, EvidenceOperations | HttpOperation | ReportPersistence> =>
  Effect.gen(function* () {
    try {
      validateIdentity(identity);
    } catch (error) {
      return yield* Effect.fail(
        error instanceof ContractError ? error : new ContractError({ message: String(error) }),
      );
    }
    const stateRef = yield* Ref.make<State>({ report: initialReport(identity), started: false });
    const scoped = Effect.scoped(
      Effect.gen(function* () {
        const operations = yield* EvidenceOperations;
        const persistence = yield* ReportPersistence;
        yield* Effect.addFinalizer((exit) =>
          Effect.gen(function* () {
            const before = yield* Ref.get(stateRef);
            if (!before.started) return;
            const cleanupExit = yield* captureEffect(() => cleanup(before));
            let current: State = Exit.isSuccess(cleanupExit)
              ? cleanupExit.value
              : {
                  ...before,
                  report: {
                    ...before.report,
                    cleanupFailures: [
                      ...before.report.cleanupFailures,
                      `cleanup: ${errorMessage(cleanupExit.cause)}`,
                    ],
                  },
                };
            const primaryFailure =
              current.report.primaryFailure ??
              (Exit.isFailure(exit) ? errorMessage(exit.cause) : undefined);
            if (primaryFailure !== undefined && current.report.primaryFailure === undefined) {
              current = { ...current, report: { ...current.report, primaryFailure } };
            }
            const writeExit = yield* captureEffect(() => persistence.write(current.report));
            if (Exit.isFailure(writeExit)) {
              current = {
                ...current,
                report: {
                  ...current.report,
                  reportWriteFailure: `report: ${errorMessage(writeExit.cause)}`,
                },
              };
            }
            yield* Ref.set(stateRef, current);
          }),
        );
        yield* Ref.update(stateRef, (state) => ({ ...state, started: true }));
        const deployedExit = yield* captureEffect(() => operations.deploy(identity));
        if (Exit.isFailure(deployedExit)) {
          const reason = errorMessage(deployedExit.cause);
          yield* Ref.update(stateRef, (state) => ({
            ...state,
            report: { ...state.report, primaryFailure: reason },
          }));
          return yield* Effect.failCause(deployedExit.cause);
        }
        const deployed = deployedExit.value;
        const target = decodeWorkerTarget(deployed, identity.workerName);
        if (!target.ok) {
          const current = yield* Ref.get(stateRef);
          yield* Ref.set(stateRef, {
            ...current,
            report: { ...current.report, primaryFailure: target.reason },
          });
          return yield* Effect.fail(new ContractError({ message: target.reason }));
        }
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          url: target.value.url,
          report: { ...state.report, url: target.value.url },
        }));
        const ready = yield* captureEffect(() => awaitReady(target.value.url));
        if (Exit.isFailure(ready)) {
          const reason = errorMessage(ready.cause);
          yield* Ref.update(stateRef, (state) => ({
            ...state,
            report: {
              ...state.report,
              readiness: failure<{ readonly attempts: number }>(reason),
              primaryFailure: reason,
            },
          }));
          return yield* Effect.failCause(ready.cause);
        }
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          report: { ...state.report, readiness: success(ready.value) },
        }));
        const conformanceExit = yield* captureEffect<ConformanceResult, EvidenceOperations>(() =>
          operations.runOfficialSuite({
            ...identity,
            url: target.value.url,
          }),
        );
        if (Exit.isFailure(conformanceExit)) {
          const reason = errorMessage(conformanceExit.cause);
          yield* Ref.update(stateRef, (state) => ({
            ...state,
            report: {
              ...state.report,
              conformance: failure<ConformanceResult>(reason),
              primaryFailure: reason,
            },
          }));
          return yield* Effect.failCause(conformanceExit.cause);
        }
        const conformance = conformanceExit.value;
        if (
          !Number.isInteger(conformance.passed) ||
          conformance.passed < 0 ||
          !Number.isInteger(conformance.skipped) ||
          conformance.skipped < 0 ||
          conformance.status.toLowerCase() === "failure" ||
          conformance.status.toLowerCase() === "failed"
        ) {
          const reason = "Official conformance reported a failing or invalid result";
          yield* Ref.update(stateRef, (state) => ({
            ...state,
            report: {
              ...state.report,
              conformance: failure(reason, conformance),
              primaryFailure: reason,
            },
          }));
          return yield* Effect.fail(new ContractError({ message: reason }));
        }
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          report: { ...state.report, conformance: success(conformance) },
        }));
        const metadataExit = yield* captureEffect<MetadataResult, EvidenceOperations>(() =>
          operations.captureMetadata({ ...identity, url: target.value.url }),
        );
        if (Exit.isFailure(metadataExit)) {
          const reason = errorMessage(metadataExit.cause);
          yield* Ref.update(stateRef, (state) => ({
            ...state,
            report: {
              ...state.report,
              metadata: failure<MetadataResult>(reason),
              primaryFailure: reason,
            },
          }));
          return yield* Effect.failCause(metadataExit.cause);
        }
        const metadata = metadataExit.value;
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          report: { ...state.report, metadata: success(metadata) },
        }));
        if (options.measurement !== undefined) {
          const measurementOptions = options.measurement;
          const measurementExit = yield* captureEffect<MeasurementEvidence, HttpOperation>(() =>
            Effect.gen(function* () {
              const latency = yield* measureLatency(measurementOptions.latency);
              const throughput = yield* measureThroughput(measurementOptions.throughput);
              return { latency, throughput } satisfies MeasurementEvidence;
            }),
          );
          if (Exit.isFailure(measurementExit)) {
            const reason = errorMessage(measurementExit.cause);
            yield* Ref.update(stateRef, (state) => ({
              ...state,
              report: {
                ...state.report,
                measurement: failure<MeasurementEvidence>(reason),
                primaryFailure: reason,
              },
            }));
            return yield* Effect.failCause(measurementExit.cause);
          }
          const measurement = measurementExit.value;
          const valid = measurement.latency.valid && measurement.throughput.valid;
          if (!valid) {
            const reason = "Measurement contained invalid or incomplete samples";
            yield* Ref.update(stateRef, (state) => ({
              ...state,
              report: {
                ...state.report,
                measurement: failure(reason, measurement),
                primaryFailure: reason,
              },
            }));
            return yield* Effect.fail(new ContractError({ message: reason }));
          }
          yield* Ref.update(stateRef, (state) => ({
            ...state,
            report: { ...state.report, measurement: success(measurement) },
          }));
        }
        return yield* Effect.void;
      }),
    );
    const result = yield* Effect.exit(scoped);
    const final = yield* Ref.get(stateRef);
    const reasons = new Set<string>();
    if (Exit.isFailure(result)) reasons.add(errorMessage(result.cause));
    if (final.report.primaryFailure !== undefined) reasons.add(final.report.primaryFailure);
    for (const reason of final.report.cleanupFailures) reasons.add(reason);
    if (final.report.reportWriteFailure !== undefined) reasons.add(final.report.reportWriteFailure);
    if (reasons.size > 0)
      return yield* Effect.fail(new ContractError({ message: [...reasons].join("; ") }));
    return yield* Effect.void;
  });
