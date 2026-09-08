import { expect, test } from "bun:test";
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { ContractError, type EvidenceReport, type RunIdentity } from "../src/contract.ts";
import { HttpOperation } from "../src/measurement.ts";
import {
  EvidenceOperations,
  ReportPersistence,
  runEvidence,
  type EvidenceOperationsService,
} from "../src/run.ts";

const identity: RunIdentity = {
  runId: "abcdef",
  stage: "c-s4l6mo-abcdef",
  workerName: "streamsy-conf-c-s4l6mo-abcdef",
  cwd: "/tmp/streamsy-conf",
  sourceSha: "source-sha",
  artifactSha256: "artifact-sha",
};

const target = { workerName: identity.workerName, url: "https://streamsy.example.workers.dev/" };

const makeServices = (
  overrides: Partial<EvidenceOperationsService> = {},
  reportFailure: boolean | "sync" = false,
) => {
  let destroyed = false;
  let report: EvidenceReport | undefined;
  const calls: Array<string> = [];
  const operations: EvidenceOperationsService = {
    deploy: () => {
      calls.push("deploy");
      return Effect.succeed(target);
    },
    destroy: () => {
      calls.push("destroy");
      destroyed = true;
      return Effect.succeed({ code: 0 });
    },
    runOfficialSuite: () => {
      calls.push("conformance");
      return Effect.succeed({ passed: 332, skipped: 6, status: "local-only" });
    },
    captureMetadata: () => {
      calls.push("metadata");
      return Effect.succeed({
        reportedScriptSize: { status: "unavailable", provenance: "fake", reason: "not captured" },
        downloadedModuleBytes: {
          status: "unavailable",
          provenance: "fake",
          reason: "not captured",
        },
        actualUploadedCompressedBytes: {
          status: "unavailable",
          provenance: "fake",
          reason: "not captured",
        },
        startupCpuMs: { status: "unavailable", provenance: "fake", reason: "not captured" },
      });
    },
    auditExactWorker: () => {
      calls.push("audit");
      return Effect.succeed({ complete: true, workers: [] });
    },
    ...overrides,
  };
  const http = {
    request: (request: { readonly method: string }) => {
      if (request.method === "GET")
        return Effect.succeed(
          destroyed
            ? { status: 404, body: "" }
            : { status: 400, body: "Stream path required: /{path}" },
        );
      if (request.method === "PUT") return Effect.succeed({ status: 201, body: "" });
      if (request.method === "HEAD") return Effect.succeed({ status: 200, body: "" });
      return Effect.succeed({ status: 204, body: "" });
    },
  };
  const persistence = {
    write: (value: EvidenceReport) => {
      report = value;
      if (reportFailure === "sync") throw new Error("sync write failed");
      return reportFailure
        ? Effect.fail(new ContractError({ message: "write failed" }))
        : Effect.succeed(undefined);
    },
  };
  const operationsLayer = Layer.succeed(EvidenceOperations, operations);
  const httpLayer = Layer.succeed(HttpOperation, http);
  const persistenceLayer = Layer.succeed(ReportPersistence, persistence);
  const layer = Layer.mergeAll(operationsLayer, httpLayer, persistenceLayer);
  return {
    layer,
    operationsLayer,
    persistenceLayer,
    calls,
    get report() {
      return report;
    },
  };
};

const runWithClock = (
  program: Effect.Effect<
    void,
    ContractError,
    EvidenceOperations | HttpOperation | ReportPersistence
  >,
  layer: Layer.Layer<EvidenceOperations | HttpOperation | ReportPersistence>,
  duration: Duration.Input = "100 seconds",
) =>
  Effect.runPromiseExit(
    Effect.gen(function* () {
      const fiber = yield* program.pipe(Effect.forkChild);
      yield* TestClock.adjust(duration);
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(Layer.mergeAll(layer, TestClock.layer()))),
  );

test("cleanup is pre-registered for partial deploy and reports the primary failure", async () => {
  const services = makeServices({
    deploy: () => {
      services.calls.push("deploy");
      return Effect.fail(new ContractError({ message: "deploy failed" }));
    },
  });
  const exit = await Effect.runPromiseExit(
    runEvidence(identity).pipe(Effect.provide(services.layer)),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual(["deploy", "destroy", "destroy", "audit"]);
  expect(services.report?.primaryFailure).toContain("deploy failed");
  expect(services.report?.gone.status).toBe("unavailable");
});

test("first destroy failure does not short-circuit destroy two or exact audit", async () => {
  let destroyCalls = 0;
  const services = makeServices({
    destroy: () => {
      destroyCalls += 1;
      services.calls.push("destroy");
      return destroyCalls === 1
        ? Effect.fail(new ContractError({ message: "first destroy" }))
        : Effect.succeed({ code: 0 });
    },
  });
  const exit = await runWithClock(runEvidence(identity), services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual([
    "deploy",
    "conformance",
    "metadata",
    "destroy",
    "destroy",
    "audit",
  ]);
  expect(services.report?.destroyFirst.status).toBe("failure");
  expect(services.report?.destroySecond.status).toBe("success");
  expect(services.report?.audit.status).toBe("success");
  expect(services.report?.cleanupFailures.some((failure) => failure.includes("destroy #1"))).toBe(
    true,
  );
});

test("non-zero destroys and a surviving exact worker fail the owned workflow", async () => {
  const nonzero = makeServices({
    destroy: () => Effect.succeed({ code: 1 }),
  });
  const nonzeroExit = await runWithClock(runEvidence(identity), nonzero.layer);
  expect(Exit.isFailure(nonzeroExit)).toBe(true);
  expect(nonzero.report?.destroyFirst).toEqual({
    status: "failure",
    reason: "destroy #1 returned non-zero code 1",
    details: { code: 1 },
  });
  const surviving = makeServices({
    auditExactWorker: () => Effect.succeed({ complete: true, workers: [identity.workerName] }),
  });
  const survivingExit = await runWithClock(runEvidence(identity), surviving.layer);
  expect(Exit.isFailure(survivingExit)).toBe(true);
  expect(surviving.report?.audit.status).toBe("failure");
});

test("a declared failing official result is recorded as a failed step", async () => {
  const services = makeServices({
    runOfficialSuite: () => Effect.succeed({ passed: 0, skipped: 6, status: "failure" }),
  });
  const exit = await runWithClock(runEvidence(identity), services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.report?.conformance.status).toBe("failure");
  expect(services.calls).toEqual(["deploy", "destroy", "destroy", "audit"]);
});

test("only the fixed affirmative conformance profile is successful", async () => {
  for (const status of ["", "error", "pending", "failed", "failure", "success"] as const) {
    const services = makeServices({
      runOfficialSuite: () => Effect.succeed({ passed: 332, skipped: 6, status }),
    });
    const exit = await runWithClock(runEvidence(identity), services.layer);
    expect(Exit.isFailure(exit)).toBe(status !== "success");
    expect(services.report?.conformance.status).toBe(status === "success" ? "success" : "failure");
  }
  const zero = makeServices({
    runOfficialSuite: () => Effect.succeed({ passed: 0, skipped: 6, status: "success" }),
  });
  const zeroExit = await runWithClock(runEvidence(identity), zero.layer);
  expect(Exit.isFailure(zeroExit)).toBe(true);
  expect(zero.report?.conformance.status).toBe("failure");
});

test("conformance failure still runs both destroys and exact audit", async () => {
  const services = makeServices({
    runOfficialSuite: () => {
      services.calls.push("conformance");
      return Effect.fail(new ContractError({ message: "conformance failed" }));
    },
  });
  const exit = await runWithClock(runEvidence(identity), services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual(["deploy", "conformance", "destroy", "destroy", "audit"]);
  expect(services.report?.primaryFailure).toContain("conformance failed");
});

test("malformed output never creates an untrusted gone target", async () => {
  const services = makeServices({
    deploy: () => {
      services.calls.push("deploy");
      return Effect.succeed({ workerName: "other", url: "https://evil.test/" });
    },
  });
  const exit = await Effect.runPromiseExit(
    runEvidence(identity).pipe(Effect.provide(services.layer)),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual(["deploy", "destroy", "destroy", "audit"]);
  expect(services.report?.gone.status).toBe("unavailable");
});

test("report persistence failure is surfaced after cleanup", async () => {
  const services = makeServices({}, true);
  const exit = await runWithClock(runEvidence(identity), services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.report).toBeDefined();
  expect(services.calls.slice(-1)).toEqual(["audit"]);
});

test("an interrupted workflow still performs cleanup", async () => {
  const started = Deferred.makeUnsafe<void>();
  const services = makeServices({
    deploy: () =>
      Effect.gen(function* () {
        services.calls.push("deploy");
        yield* Deferred.succeed(started, undefined);
        return yield* Effect.never;
      }),
  });
  const exit = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const fiber = yield* runEvidence(identity).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(services.layer)),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual(["deploy", "destroy", "destroy", "audit"]);
  expect(services.report?.primaryFailure).toContain("interrupted");
});

const interruptAt = async (
  program: Parameters<typeof runEvidence>[1] extends never ? never : ReturnType<typeof runEvidence>,
  started: Deferred.Deferred<void>,
  layer: Layer.Layer<EvidenceOperations | HttpOperation | ReportPersistence>,
) =>
  Effect.runPromiseExit(
    Effect.gen(function* () {
      const fiber = yield* program.pipe(Effect.forkChild);
      const ticker = yield* Effect.forever(
        Effect.gen(function* () {
          yield* TestClock.adjust("2 seconds");
          yield* Effect.yieldNow;
        }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      const interruption = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
      yield* Fiber.join(interruption);
      yield* Fiber.interrupt(ticker);
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(Layer.mergeAll(layer, TestClock.layer()))),
  );

test("external interruption preserves typed and synchronous report failures", async () => {
  for (const reportFailure of [true, "sync"] as const) {
    const started = Deferred.makeUnsafe<void>();
    const services = makeServices(
      {
        deploy: () =>
          Effect.gen(function* () {
            services.calls.push("deploy");
            yield* Deferred.succeed(started, undefined);
            return yield* Effect.never;
          }),
      },
      reportFailure,
    );
    const exit = await interruptAt(runEvidence(identity), started, services.layer);
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const message = Cause.pretty(exit.cause);
      expect(message).toContain("interrupted");
      expect(message).toContain("write");
    }
    expect(services.calls).toEqual(["deploy", "destroy", "destroy", "audit"]);
  }
});

test("external interruption preserves cleanup and report failures together", async () => {
  const started = Deferred.makeUnsafe<void>();
  const services = makeServices(
    {
      deploy: () =>
        Effect.gen(function* () {
          services.calls.push("deploy");
          yield* Deferred.succeed(started, undefined);
          return yield* Effect.never;
        }),
      destroy: () => Effect.fail(new ContractError({ message: "destroy secondary" })),
      auditExactWorker: () => Effect.fail(new ContractError({ message: "audit secondary" })),
    },
    true,
  );
  const exit = await interruptAt(runEvidence(identity), started, services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isFailure(exit)) {
    const message = Cause.pretty(exit.cause);
    expect(message).toContain("interrupted");
    expect(message).toContain("destroy secondary");
    expect(message).toContain("audit secondary");
    expect(message).toContain("write failed");
  }
});

test("interrupted attempted conformance and metadata steps are reported as failures", async () => {
  for (const step of ["conformance", "metadata"] as const) {
    const started = Deferred.makeUnsafe<void>();
    let services: ReturnType<typeof makeServices>;
    const overrides =
      step === "conformance"
        ? {
            runOfficialSuite: () =>
              Effect.gen(function* () {
                services.calls.push("conformance");
                yield* Deferred.succeed(started, undefined);
                return yield* Effect.never;
              }),
          }
        : {
            captureMetadata: () =>
              Effect.gen(function* () {
                services.calls.push("metadata");
                yield* Deferred.succeed(started, undefined);
                return yield* Effect.never;
              }),
          };
    services = makeServices(overrides);
    const exit = await interruptAt(runEvidence(identity), started, services.layer);
    expect(Exit.isFailure(exit)).toBe(true);
    const stepResult = services.report?.[step];
    expect(stepResult?.status).toBe("failure");
    if (stepResult?.status === "failure") expect(stepResult.reason).toContain("interrupted");
  }
});

test("interrupted configured measurement is not reported as unavailable", async () => {
  const started = Deferred.makeUnsafe<void>();
  const services = makeServices();
  const options = {
    measurement: {
      latency: { baseUrl: target.url, paths: ["/streams/a"], concurrency: 1 },
      throughput: { baseUrl: target.url, trials: 1, postsPerTrial: 1, concurrency: 1 },
    },
  } as const;
  const measurementHttp = Layer.succeed(HttpOperation, {
    request: (request: { readonly method: string }) =>
      request.method === "GET"
        ? Effect.succeed({ status: 400, body: "Stream path required: /{path}" })
        : request.method === "PUT"
          ? Effect.gen(function* () {
              yield* Deferred.succeed(started, undefined);
              return yield* Effect.never;
            })
          : Effect.succeed({ status: 200, body: "" }),
  });
  const exit = await interruptAt(
    runEvidence(identity, options),
    started,
    Layer.mergeAll(services.operationsLayer, services.persistenceLayer, measurementHttp),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.report?.measurement.status).toBe("failure");
  if (services.report?.measurement.status === "failure")
    expect(services.report.measurement.reason).toContain("interrupted");
});

test("synchronous cleanup throws do not skip later cleanup or reporting", async () => {
  const services = makeServices({
    destroy: () => {
      services.calls.push("destroy");
      throw new Error("sync destroy failure");
    },
  });
  const exit = await runWithClock(runEvidence(identity), services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual([
    "deploy",
    "conformance",
    "metadata",
    "destroy",
    "destroy",
    "audit",
  ]);
  expect(services.report).toBeDefined();
  expect(services.report?.cleanupFailures.join(" ")).toContain("destroy #1");
});

test("primary and report failures are both observable", async () => {
  const services = makeServices(
    {
      deploy: () => Effect.fail(new ContractError({ message: "original deploy failure" })),
    },
    true,
  );
  const exit = await runWithClock(runEvidence(identity), services.layer);
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.report?.primaryFailure).toContain("original deploy failure");
  if (Exit.isFailure(exit)) expect(Cause.pretty(exit.cause)).toContain("report: ContractError");
});

test("fake measurement success and invalid body failures are part of the owned workflow", async () => {
  const options = {
    measurement: {
      latency: { baseUrl: target.url, paths: ["/streams/a"], concurrency: 1 },
      throughput: { baseUrl: target.url, trials: 1, postsPerTrial: 1, concurrency: 1 },
    },
  } as const;
  const successServices = makeServices();
  let measurementGetCalls = 0;
  const measurementHttp = Layer.succeed(HttpOperation, {
    request: (request: { readonly method: string }) =>
      Effect.sleep("1 millis").pipe(
        Effect.as(
          request.method === "GET"
            ? measurementGetCalls++ < 5
              ? { status: 400, body: "Stream path required: /{path}" }
              : { status: 404, body: "" }
            : {
                status: request.method === "PUT" ? 201 : request.method === "HEAD" ? 200 : 204,
                body: "",
              },
        ),
      ),
  });
  const successExit = await runWithClock(
    runEvidence(identity, options),
    Layer.mergeAll(successServices.layer, measurementHttp),
  );
  expect(Exit.isSuccess(successExit)).toBe(true);
  expect(successServices.report?.measurement.status).toBe("success");
  if (successServices.report?.measurement.status === "success") {
    expect(successServices.report.measurement.value.latency.firstPutBody).toBe("x");
    expect(successServices.report.measurement.value.latency.step0FirstPutBody).toBe("");
  }
  const failingServices = makeServices();
  const failingHttp = Layer.succeed(HttpOperation, {
    request: (request: { readonly method: string }) =>
      request.method === "HEAD"
        ? Effect.fail(new ContractError({ message: "body failed" }))
        : request.method === "GET"
          ? Effect.succeed({ status: 400, body: "Stream path required: /{path}" })
          : Effect.succeed({ status: request.method === "PUT" ? 201 : 204, body: "" }),
  });
  const failingExit = await runWithClock(
    runEvidence(identity, options),
    Layer.mergeAll(failingServices.layer, failingHttp),
  );
  expect(Exit.isFailure(failingExit)).toBe(true);
  expect(failingServices.report?.measurement.status).toBe("failure");
  expect(failingServices.calls.slice(-3)).toEqual(["destroy", "destroy", "audit"]);
});

test("preflight failure performs no remote operation or cleanup", async () => {
  const services = makeServices();
  const exit = await Effect.runPromiseExit(
    runEvidence({ ...identity, stage: "not-a-stage" }).pipe(Effect.provide(services.layer)),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  expect(services.calls).toEqual([]);
  expect(services.report).toBeUndefined();
});
