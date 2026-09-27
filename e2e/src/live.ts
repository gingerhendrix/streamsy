/**
 * Live adapters for the deployed e2e run.
 *
 * `runEvidence` in run.ts owns the order: deploy, readiness, official suite,
 * metadata, then destroy twice, the 404 probe and the audit. This module
 * supplies the real services it needs. Deploy and destroy call the Alchemy v2
 * CLI with the local `.alchemy/` state store. The official suite runs
 * `packages/conformance-tests` `test:deployed` against the Worker URL.
 */
import { createHash, randomBytes } from "node:crypto";
import { Clock, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import {
  ContractError,
  STACK_NAME,
  StackOutput,
  expectedWorkerName,
  stageName,
  type EvidenceReport,
  type MetadataAvailability,
  type RunIdentity,
} from "./contract.ts";
import { HttpOperation, type HttpRequest } from "./measurement.ts";
import {
  EvidenceOperations,
  ReportPersistence,
  type ConformanceResult,
  type MetadataResult,
} from "./run.ts";

/** Paths the live run reads and writes. `root` is the e2e package folder. */
export interface LivePaths {
  readonly root: string;
  readonly conformanceTests: string;
  readonly workerArtifact: string;
  readonly reports: string;
}

export const livePaths = (root: string): LivePaths => ({
  root,
  conformanceTests: `${root}/../packages/conformance-tests`,
  workerArtifact: `${root}/../packages/conformance-tests/dist/worker/worker.js`,
  reports: `${root}/reports`,
});

const contractError = (message: string) => (cause: { readonly message: string }) =>
  new ContractError({ message: `${message}: ${cause.message}` });

/** Alchemy v2 runs under Node. Under Bun its published build misses a runtime dependency. */
const alchemy = (paths: LivePaths, args: ReadonlyArray<string>) =>
  ChildProcess.make("node", [`${paths.root}/node_modules/alchemy/bin/cli.js`, ...args], {
    cwd: paths.root,
    stdout: "inherit",
    stderr: "inherit",
  });

const exitCodeOf = (command: ChildProcess.Command, label: string) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const code = yield* spawner.exitCode(command).pipe(Effect.mapError(contractError(label)));
    return Number(code);
  });

const stageDirectory = (paths: LivePaths, stage: string) =>
  `${paths.root}/.alchemy/state/${STACK_NAME}/${stage}`;

const STACK_OUTPUT_FILE = "__stack_output__.json";

/** The subset of the vitest JSON report that the run records. */
const VitestReport = Schema.Struct({
  success: Schema.Boolean,
  numPassedTests: Schema.Finite,
  numPendingTests: Schema.Finite,
  numTodoTests: Schema.Finite,
});

const decodeVitestReport = (text: string): ConformanceResult => {
  const report = Schema.decodeSync(Schema.fromJsonString(VitestReport))(text);
  return {
    passed: report.numPassedTests,
    skipped: report.numPendingTests + report.numTodoTests,
    status: report.success ? "success" : "failure",
  };
};

const notQueried: MetadataAvailability = {
  status: "unavailable",
  provenance: "e2e runner",
  reason: "The deployed e2e run does not query the Cloudflare metadata API",
};

/** Real deploy, destroy, official suite and state audit for one stage. */
export const liveOperations = (paths: LivePaths) =>
  Layer.effect(
    EvidenceOperations,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const provide = <A, E>(
        effect: Effect.Effect<A, E, ChildProcessSpawner.ChildProcessSpawner>,
      ): Effect.Effect<A, E> =>
        Effect.provideService(effect, ChildProcessSpawner.ChildProcessSpawner, spawner);
      const readText = (file: string, label: string) =>
        fs.readFileString(file).pipe(Effect.mapError(contractError(label)));

      return {
        deploy: (input) =>
          provide(
            Effect.gen(function* () {
              const code = yield* exitCodeOf(
                alchemy(paths, ["deploy", "--stage", input.stage, "--yes", "--no-input"]),
                "alchemy deploy",
              );
              if (code !== 0) {
                return yield* new ContractError({ message: `alchemy deploy exited with ${code}` });
              }
              const file = `${stageDirectory(paths, input.stage)}/${STACK_OUTPUT_FILE}`;
              const text = yield* readText(file, "Stack output");
              return yield* Schema.decodeEffect(Schema.fromJsonString(StackOutput))(text).pipe(
                Effect.mapError(
                  () => new ContractError({ message: "Stack output was missing or malformed" }),
                ),
              );
            }),
          ),
        destroy: (input) =>
          provide(
            exitCodeOf(
              alchemy(paths, ["destroy", "--stage", input.stage, "--yes", "--no-input"]),
              "alchemy destroy",
            ).pipe(Effect.map((code) => ({ code }))),
          ),
        runOfficialSuite: (input) =>
          provide(
            Effect.gen(function* () {
              yield* fs
                .makeDirectory(paths.reports, { recursive: true })
                .pipe(Effect.mapError(contractError("Report directory")));
              const output = `${paths.reports}/${input.stage}.vitest.json`;
              yield* exitCodeOf(
                ChildProcess.make(
                  "bun",
                  [
                    "run",
                    "test:deployed",
                    "--reporter=dot",
                    "--reporter=json",
                    `--outputFile.json=${output}`,
                  ],
                  {
                    cwd: paths.conformanceTests,
                    env: { SERVER_BASE_URL: input.url },
                    extendEnv: true,
                    stdout: "inherit",
                    stderr: "inherit",
                  },
                ),
                "official suite",
              );
              const text = yield* readText(output, "Official suite report");
              return yield* Effect.try({
                try: () => decodeVitestReport(text),
                catch: () => new ContractError({ message: "Official suite report was malformed" }),
              });
            }),
          ),
        captureMetadata: () =>
          Effect.succeed({
            reportedScriptSize: notQueried,
            downloadedModuleBytes: notQueried,
            actualUploadedCompressedBytes: notQueried,
            startupCpuMs: notQueried,
          } satisfies MetadataResult),
        // After destroy, Alchemy's local state for the stage must hold no
        // resource records. Any record left means the Worker may still exist.
        auditExactWorker: (input) =>
          Effect.gen(function* () {
            const directory = stageDirectory(paths, input.stage);
            if (!(yield* fs.exists(directory))) return { complete: true, workers: [] };
            const entries = yield* fs.readDirectory(directory);
            const resources = entries.filter(
              (entry) => entry.endsWith(".json") && entry !== STACK_OUTPUT_FILE,
            );
            return { complete: true, workers: resources.length > 0 ? [input.workerName] : [] };
          }).pipe(Effect.mapError(contractError("State audit"))),
      };
    }),
  );

/** Plain fetch. The readiness and 404 probes use it. */
export const liveHttp = Layer.succeed(HttpOperation, {
  request: (request: HttpRequest) =>
    Effect.tryPromise({
      try: (signal) =>
        // oxlint-disable-next-line effecttsgo/global-fetch-in-effect -- This adapter is the HTTP edge behind the HttpOperation service.
        fetch(request.url, {
          method: request.method,
          body: request.body,
          headers: request.headers,
          signal,
        }).then((response) => response.text().then((body) => ({ status: response.status, body }))),
      catch: (cause) =>
        new ContractError({ message: `${request.method} ${request.url}: ${String(cause)}` }),
    }),
});

/** Writes the evidence report as JSON under `reports/`. */
export const liveReports = (paths: LivePaths) =>
  Layer.effect(
    ReportPersistence,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      return {
        write: (report: EvidenceReport) =>
          fs.makeDirectory(paths.reports, { recursive: true }).pipe(
            Effect.andThen(
              fs.writeFileString(
                `${paths.reports}/${report.stage}.json`,
                `${JSON.stringify(report, null, 2)}\n`,
              ),
            ),
            Effect.tap(() =>
              Effect.logInfo(`Evidence report: ${paths.reports}/${report.stage}.json`),
            ),
            Effect.mapError(contractError("Report write")),
          ),
      };
    }),
  );

/** A fresh stage, the planned Worker name, the source commit and the artifact hash. */
export const liveIdentity = (paths: LivePaths) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const artifact = yield* fs
      .readFile(paths.workerArtifact)
      .pipe(
        Effect.mapError(
          contractError("Missing worker artifact; run bun run build:worker in conformance-tests"),
        ),
      );
    const sourceSha = yield* spawner
      .string(ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: paths.root }))
      .pipe(
        Effect.map((text) => text.trim()),
        Effect.mapError(contractError("git rev-parse")),
      );
    const runId = randomBytes(3).toString("hex");
    const now = yield* Clock.currentTimeMillis;
    const stage = yield* Effect.try({
      try: () => stageName(Math.floor(now / 1000), runId),
      catch: (cause) => new ContractError({ message: String(cause) }),
    });
    return {
      runId,
      stage,
      workerName: expectedWorkerName(stage),
      cwd: path.resolve(paths.root),
      sourceSha,
      artifactSha256: createHash("sha256").update(artifact).digest("hex"),
    } satisfies RunIdentity;
  });
