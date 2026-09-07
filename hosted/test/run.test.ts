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
  reportFailure = false,
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
      return reportFailure
        ? Effect.fail(new ContractError({ message: "write failed" }))
        : Effect.succeed(undefined);
    },
  };
  const layer = Layer.mergeAll(
    Layer.succeed(EvidenceOperations, operations),
    Layer.succeed(HttpOperation, http),
    Layer.succeed(ReportPersistence, persistence),
  );
  return {
    layer,
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
