/* oxlint-disable typescript/consistent-return -- Effect failure branches use return-yield* for never-successful effects; successful probes return their value. */
import { Cause, Context, Effect, Exit, Ref, Schedule } from "effect";
import {
  ContractError,
  HOSTED_STATUS,
  type EvidenceReport,
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
import { HttpOperation, type FullResponse } from "./measurement.ts";

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
  measurement: unavailable("Live measurement adapters are not enabled in C-local"),
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

const errorMessage = (cause: Cause.Cause<ContractError>): string => Cause.pretty(cause);

const exitStep = (
  exit: Exit.Exit<{ readonly code: number }, ContractError>,
  label: string,
): ExitStep => {
  if (Exit.isSuccess(exit)) return { result: success(exit.value) };
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
    const first = yield* Effect.exit(operations.destroy(destroyInput));
    const firstStep = exitStep(first, "destroy #1");
    if (firstStep.error !== undefined) nextFailures.push(firstStep.error);
    const second = yield* Effect.exit(operations.destroy(destroyInput));
    const secondStep = exitStep(second, "destroy #2");
    if (secondStep.error !== undefined) nextFailures.push(secondStep.error);
    let gone = unavailable<{ readonly status: number }>("Validated worker URL was unavailable");
    if (state.url !== undefined) {
      const goneExit = yield* Effect.exit(awaitGone(state.url));
      if (Exit.isSuccess(goneExit)) gone = success({ status: 404 });
      else {
        gone = failure(`gone: ${errorMessage(goneExit.cause)}`);
        nextFailures.push(`gone: ${errorMessage(goneExit.cause)}`);
      }
    }
    const auditExit = yield* Effect.exit(operations.auditExactWorker(state.report));
    let audit = unavailable<AuditResult>("Exact-worker audit was unavailable");
    if (Exit.isSuccess(auditExit)) {
      audit = auditExit.value.complete
        ? success(auditExit.value)
        : failure("Exact-worker audit was incomplete");
      if (!auditExit.value.complete)
        nextFailures.push("audit: incomplete authenticated pagination");
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
            if (before.started) {
              const cleaned = yield* cleanup(before);
              const withPrimary =
                before.report.primaryFailure === undefined && Exit.isFailure(exit)
                  ? {
                      ...cleaned.report,
                      primaryFailure: "Workflow exited before completing its typed operation",
                    }
                  : cleaned.report;
              const writeExit = yield* Effect.exit(persistence.write(withPrimary));
              const finalReport = Exit.isSuccess(writeExit)
                ? withPrimary
                : {
                    ...withPrimary,
                    reportWriteFailure: `report: ${errorMessage(writeExit.cause)}`,
                  };
              yield* Ref.set(stateRef, { ...cleaned, report: finalReport });
            }
          }),
        );
        yield* Ref.update(stateRef, (state) => ({ ...state, started: true }));
        const deployed = yield* operations.deploy(identity);
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
        const ready = yield* Effect.exit(awaitReady(target.value.url));
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
          return yield* Effect.fail(new ContractError({ message: reason }));
        }
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          report: { ...state.report, readiness: success(ready.value) },
        }));
        const conformance = yield* operations.runOfficialSuite({
          ...identity,
          url: target.value.url,
        });
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          report: { ...state.report, conformance: success(conformance) },
        }));
        const metadata = yield* operations.captureMetadata({ ...identity, url: target.value.url });
        yield* Ref.update(stateRef, (state) => ({
          ...state,
          report: { ...state.report, metadata: success(metadata) },
        }));
        return yield* Effect.void;
      }).pipe(
        Effect.tapError((error) =>
          Ref.update(stateRef, (state) => ({
            ...state,
            report:
              state.report.primaryFailure === undefined
                ? { ...state.report, primaryFailure: error.message }
                : state.report,
          })),
        ),
      ),
    );
    const result = yield* Effect.exit(scoped);
    const final = yield* Ref.get(stateRef);
    if (final.report.reportWriteFailure !== undefined && Exit.isSuccess(result)) {
      return yield* Effect.fail(new ContractError({ message: final.report.reportWriteFailure }));
    }
    if (Exit.isFailure(result))
      return yield* Effect.fail(
        new ContractError({ message: final.report.primaryFailure ?? "Evidence workflow failed" }),
      );
    return yield* Effect.void;
  });
