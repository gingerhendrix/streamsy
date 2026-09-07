import { expect, test } from "bun:test";
import { Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import {
  ContractError,
  decodeWorkerTarget,
  expectedWorkerName,
  isValidStage,
  makeStage,
} from "../src/contract.ts";
import {
  HttpOperation,
  measureLatency,
  measureThroughput,
  percentile,
  summarize,
  type FullResponse,
  type HttpRequest,
} from "../src/measurement.ts";
import { awaitGone, awaitReady } from "../src/run.ts";

test("stage generation uses the fixed short format and rejects unsafe inputs", () => {
  const stage = makeStage(1_800_000_000, "abcdef");
  expect(stage).toBe("c-tro8w0-abcdef");
  expect(stage.length).toBeLessThanOrEqual(16);
  expect(isValidStage(stage)).toBe(true);
  expect(() => makeStage(-1, "abcdef")).toThrow(ContractError);
  expect(() => makeStage(1, "ABCDEF")).toThrow(ContractError);
  expect(isValidStage("c-s4l6mo-abcdef-collision")).toBe(false);
  expect(expectedWorkerName(stage)).toBe(`streamsy-conf-${stage}`);
});

test("worker output decoding is schema-backed and distrusts arbitrary targets", () => {
  const stage = makeStage(1_800_000_000, "abcdef");
  const workerName = expectedWorkerName(stage);
  expect(
    decodeWorkerTarget({ workerName, url: "https://streamsy.example.workers.dev" }, workerName),
  ).toEqual({
    ok: true,
    value: { workerName, url: "https://streamsy.example.workers.dev/" },
  });
  expect(
    decodeWorkerTarget({ workerName, url: "http://streamsy.example.workers.dev/" }, workerName).ok,
  ).toBe(false);
  expect(
    decodeWorkerTarget(
      { workerName: "other", url: "https://streamsy.example.workers.dev/" },
      workerName,
    ).ok,
  ).toBe(false);
  expect(
    decodeWorkerTarget(
      { workerName, url: "https://streamsy.example.workers.dev/?token=secret" },
      workerName,
    ).ok,
  ).toBe(false);
  expect(decodeWorkerTarget(undefined, workerName).ok).toBe(false);
});

test("nearest-rank percentile uses index 284 for n=300 at p95", () => {
  const samples = Array.from({ length: 300 }, (_, index) => index + 1);
  expect(percentile(samples, 95)).toBe(285);
  expect(summarize([1, 3, 2, 5, 4]).p50).toBe(3);
  expect(() => percentile([], 95)).toThrow(ContractError);
});

test("latency preserves historical all-first-then-warm ordering and paired samples", async () => {
  const requests: Array<string> = [];
  const layer = Layer.succeed(HttpOperation, {
    request: (request) =>
      Effect.sync(() => {
        requests.push(request.method);
        return { status: 200, body: "" };
      }),
  });
  const result = await Effect.runPromise(
    measureLatency({ baseUrl: "https://example.test", paths: ["/a", "/b"], concurrency: 2 }).pipe(
      Effect.provide(layer),
    ),
  );
  expect(requests).toEqual(["PUT", "PUT", "HEAD", "HEAD"]);
  expect(result.protocol).toBe("after-all-first");
  expect(result.warmOrdering).toBe("after-all-first");
  expect(result.firstPutBody).toBe("x");
  expect(result.step0FirstPutBody).toBe("");
  expect(result.pairs).toHaveLength(2);
  expect(result.valid).toBe(true);
});

test("latency deltas preserve the historical first-minus-warm sign", async () => {
  const exit = await Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* measureLatency({
        baseUrl: "https://example.test",
        paths: ["/a"],
        concurrency: 1,
      }).pipe(Effect.forkChild);
      yield* TestClock.adjust("1 second");
      return yield* Fiber.join(fiber);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(HttpOperation, {
            request: (request) =>
              Effect.sleep(request.method === "PUT" ? "10 millis" : "3 millis").pipe(
                Effect.as({ status: 200, body: "" }),
              ),
          }),
          TestClock.layer(),
        ),
      ),
    ),
  );
  expect(exit.pairs[0]?.firstMinusWarmMs).toBe(7);
  expect(exit.deltas.status).toBe("success");
});

test("latency does not produce a valid aggregate after a failed response", async () => {
  const layer = Layer.succeed(HttpOperation, {
    request: (request) =>
      Effect.succeed({ status: request.method === "PUT" ? 500 : 200, body: "failure" }),
  });
  const result = await Effect.runPromise(
    measureLatency({ baseUrl: "https://example.test", paths: ["/a"], concurrency: 1 }).pipe(
      Effect.provide(layer),
    ),
  );
  expect(result.valid).toBe(false);
  expect(result.first.status).toBe("failure");
  expect(result.deltas.status).toBe("failure");
});

test("latency rejects initial PUT, HEAD, and body failures", async () => {
  const result = await Effect.runPromise(
    measureLatency({ baseUrl: "https://example.test", paths: ["/a"], concurrency: 1 }).pipe(
      Effect.provide(
        Layer.succeed(HttpOperation, {
          request: (request) =>
            request.method === "PUT"
              ? Effect.succeed({ status: 500, body: "put failed" })
              : Effect.fail(new ContractError({ message: "body failed" })),
        }),
      ),
    ),
  );
  expect(result.valid).toBe(false);
  expect(result.first.status).toBe("failure");
  expect(result.warm.status).toBe("failure");
  expect(result.pairs[0]?.error).toContain("body failed");
});

test("throughput records invalid create and POST responses", async () => {
  let postCalls = 0;
  const result = await Effect.runPromise(
    measureThroughput({
      baseUrl: "https://example.test",
      trials: 1,
      postsPerTrial: 2,
      concurrency: 1,
    }).pipe(
      Effect.provide(
        Layer.succeed(HttpOperation, {
          request: (request) => {
            if (request.method === "PUT")
              return Effect.succeed({ status: 500, body: "create failed" });
            postCalls += 1;
            return postCalls === 1
              ? Effect.fail(new ContractError({ message: "post body failed" }))
              : Effect.succeed({ status: 201, body: "" });
          },
        }),
      ),
    ),
  );
  expect(result.valid).toBe(false);
  expect(result.medianRate).toBeUndefined();
  expect(result.trials[0]?.errors).toEqual(["create: HTTP 500", "post 0: post body failed"]);
});

test("throughput preserves trial ordering and historical diagnostics", async () => {
  const methods: Array<string> = [];
  const result = await Effect.runPromise(
    measureThroughput({
      baseUrl: "https://example.test",
      trials: 5,
      postsPerTrial: 1,
      concurrency: 1,
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(HttpOperation, {
            request: (request) =>
              Effect.gen(function* () {
                methods.push(request.method);
                if (request.method === "POST") yield* TestClock.adjust("1 second");
                return { status: request.method === "PUT" ? 201 : 204, body: "" };
              }),
          }),
          TestClock.layer(),
        ),
      ),
    ),
  );
  expect(methods).toEqual([
    "PUT",
    "POST",
    "PUT",
    "POST",
    "PUT",
    "POST",
    "PUT",
    "POST",
    "PUT",
    "POST",
  ]);
  expect(result.valid).toBe(true);
  expect(result.medianRate).toBe(1);
  expect(result.diagnostics.step0Rate).toBe(54.60448413131066);
  expect(result.diagnostics.priorReferenceRate).toBe(91);
});

const clockLayer = (
  request: (request: HttpRequest) => Effect.Effect<FullResponse, ContractError>,
) => Layer.mergeAll(Layer.succeed(HttpOperation, { request }), TestClock.layer());

test("readiness requires five consecutive exact full-body matches", async () => {
  let calls = 0;
  const program = Effect.gen(function* () {
    const fiber = yield* awaitReady("https://example.test/").pipe(Effect.forkChild);
    yield* TestClock.adjust("8 seconds");
    return yield* Fiber.join(fiber);
  });
  const result = await Effect.runPromise(
    program.pipe(
      Effect.provide(
        clockLayer(() => {
          calls += 1;
          return Effect.succeed({ status: 400, body: "Stream path required: /{path}" });
        }),
      ),
    ),
  );
  expect(result.attempts).toBe(5);
  expect(calls).toBe(5);
});

test("readiness resets on a transport failure and gone requires 404", async () => {
  let calls = 0;
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* awaitReady("https://example.test/").pipe(Effect.forkChild);
      yield* TestClock.adjust("30 seconds");
      return yield* Fiber.join(fiber);
    }).pipe(
      Effect.provide(
        clockLayer(() => {
          calls += 1;
          if (calls === 3) return Effect.fail(new ContractError({ message: "transport" }));
          return Effect.succeed({ status: 400, body: "Stream path required: /{path}" });
        }),
      ),
    ),
  );
  expect(result.attempts).toBe(8);
  expect(calls).toBe(8);
  const gone = await Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* awaitGone("https://example.test/").pipe(Effect.forkChild);
      yield* TestClock.adjust("8 seconds");
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(clockLayer(() => Effect.succeed({ status: 404, body: "" })))),
  );
  expect(gone.attempts).toBe(5);
});

test("a body that never completes is bounded by the probe timeout", async () => {
  const exit = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const fiber = yield* awaitReady("https://example.test/").pipe(Effect.forkChild);
      yield* TestClock.adjust("60 seconds");
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(clockLayer(() => Effect.never))),
  );
  expect(Exit.isFailure(exit)).toBe(true);
});
