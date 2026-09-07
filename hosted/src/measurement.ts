import { Clock, Context, Effect, Result } from "effect";
import {
  CONFORMANCE_LONG_POLL_TIMEOUT_MS,
  ContractError,
  type StepResult,
  success,
  failure,
} from "./contract.ts";

export interface HttpRequest {
  readonly method: "GET" | "HEAD" | "PUT" | "POST";
  readonly url: string;
  readonly body?: string;
  readonly headers?: Readonly<Record<string, string>>;
}

export interface FullResponse {
  readonly status: number;
  readonly body: string;
}

export interface HttpOperationService {
  readonly request: (request: HttpRequest) => Effect.Effect<FullResponse, ContractError>;
}

export class HttpOperation extends Context.Service<HttpOperation, HttpOperationService>()(
  "@streamsy/hosted/HttpOperation",
) {}

export interface LatencySample {
  readonly path: string;
  readonly phase: "first" | "warm";
  readonly elapsedMs?: number;
  readonly status?: number;
  readonly error?: string;
}

export interface LatencyPair {
  readonly path: string;
  readonly firstMs?: number;
  readonly warmMs?: number;
  readonly firstMinusWarmMs?: number;
  readonly firstStatus?: number;
  readonly warmStatus?: number;
  readonly error?: string;
}

export interface Summary {
  readonly count: number;
  readonly min: number;
  readonly max: number;
  readonly mean: number;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
}

export interface LatencyMeasurement {
  readonly protocol: "after-all-first";
  readonly warmOrdering: "after-all-first";
  readonly firstPutBody: "x";
  readonly step0FirstPutBody: "";
  readonly concurrency: number;
  readonly pairs: ReadonlyArray<LatencyPair>;
  readonly first: StepResult<Summary>;
  readonly warm: StepResult<Summary>;
  readonly deltas: StepResult<Summary>;
  readonly valid: boolean;
}

export const percentile = (samples: ReadonlyArray<number>, percentileValue: number): number => {
  if (
    samples.length === 0 ||
    !Number.isFinite(percentileValue) ||
    percentileValue < 0 ||
    percentileValue > 100 ||
    samples.some((sample) => !Number.isFinite(sample))
  ) {
    throw new ContractError({ message: "Percentile requires a nonempty finite sample set" });
  }
  const sorted = samples.toSorted((left, right) => left - right);
  const index = Math.max(0, Math.ceil((percentileValue * sorted.length) / 100) - 1);
  const value = sorted[index];
  if (value === undefined) throw new ContractError({ message: "Percentile index was invalid" });
  return value;
};

export const summarize = (samples: ReadonlyArray<number>): Summary => {
  if (samples.length === 0 || samples.some((sample) => !Number.isFinite(sample))) {
    throw new ContractError({ message: "Summary requires a nonempty finite sample set" });
  }
  return {
    count: samples.length,
    min: Math.min(...samples),
    max: Math.max(...samples),
    mean: samples.reduce((total, sample) => total + sample, 0) / samples.length,
    p50: percentile(samples, 50),
    p95: percentile(samples, 95),
    p99: percentile(samples, 99),
  };
};

const timedRequest = Effect.fn("Hosted.measure.timedRequest")(function* (
  request: HttpRequest,
  path: string,
  phase: LatencySample["phase"],
) {
  const http = yield* HttpOperation;
  const start = yield* Clock.currentTimeNanos;
  const result = yield* Effect.result(http.request(request));
  const end = yield* Clock.currentTimeNanos;
  const elapsedMs = Number(end - start) / 1_000_000;
  if (Result.isFailure(result)) {
    return { path, phase, elapsedMs, error: result.failure.message } satisfies LatencySample;
  }
  return { path, phase, elapsedMs, status: result.success.status } satisfies LatencySample;
});

const sampleSummary = (
  samples: ReadonlyArray<LatencySample>,
  phase: LatencySample["phase"],
): StepResult<Summary> => {
  const values = samples
    .filter(
      (sample) =>
        sample.phase === phase &&
        sample.error === undefined &&
        sample.status !== undefined &&
        sample.status >= 200 &&
        sample.status < 300,
    )
    .map((sample) => sample.elapsedMs)
    .filter((sample): sample is number => sample !== undefined);
  const invalid = samples.some(
    (sample) =>
      sample.phase === phase &&
      (sample.error !== undefined ||
        sample.status === undefined ||
        sample.status < 200 ||
        sample.status >= 300),
  );
  if (invalid || values.length !== samples.filter((sample) => sample.phase === phase).length) {
    return failure(`${phase} request set contained a non-2xx, transport, or body failure`);
  }
  try {
    return success(summarize(values));
  } catch (error) {
    return failure(error instanceof Error ? error.message : String(error));
  }
};

export interface LatencyOptions {
  readonly baseUrl: string;
  readonly paths?: ReadonlyArray<string>;
  readonly concurrency?: number;
}

export const measureLatency = (
  options: LatencyOptions,
): Effect.Effect<LatencyMeasurement, ContractError, HttpOperation> =>
  Effect.gen(function* () {
    const paths =
      options.paths ?? Array.from({ length: 300 }, (_, index) => `/streams/latency-${index}`);
    const concurrency = options.concurrency ?? 16;
    const writes = yield* Effect.forEach(
      paths,
      (path) =>
        timedRequest(
          {
            method: "PUT",
            url: new URL(path, options.baseUrl).toString(),
            body: "x",
            headers: { "content-type": "text/plain" },
          },
          path,
          "first",
        ),
      { concurrency },
    );
    const warms = yield* Effect.forEach(
      paths,
      (path) =>
        timedRequest(
          {
            method: "HEAD",
            url: new URL(path, options.baseUrl).toString(),
          },
          path,
          "warm",
        ),
      { concurrency },
    );
    const pairs = paths.map((path, index) => {
      const first = writes[index];
      const warm = warms[index];
      const error =
        first?.error ??
        warm?.error ??
        (first?.status !== undefined && (first.status < 200 || first.status >= 300)
          ? `first: HTTP ${first.status}`
          : undefined) ??
        (warm?.status !== undefined && (warm.status < 200 || warm.status >= 300)
          ? `warm: HTTP ${warm.status}`
          : undefined);
      const pair: LatencyPair = {
        path,
        firstMs: first?.elapsedMs,
        warmMs: warm?.elapsedMs,
        firstMinusWarmMs:
          first?.elapsedMs !== undefined && warm?.elapsedMs !== undefined
            ? first.elapsedMs - warm.elapsedMs
            : undefined,
        firstStatus: first?.status,
        warmStatus: warm?.status,
        error,
      };
      return pair;
    });
    const first = sampleSummary(writes, "first");
    const warm = sampleSummary(warms, "warm");
    let deltas: StepResult<Summary>;
    if (pairs.every((pair) => pair.firstMinusWarmMs !== undefined && pair.error === undefined)) {
      try {
        deltas = success(
          summarize(
            pairs
              .map((pair) => pair.firstMinusWarmMs)
              .filter((value): value is number => value !== undefined),
          ),
        );
      } catch (error) {
        deltas = failure(error instanceof Error ? error.message : String(error));
      }
    } else {
      deltas = failure("Paired latency deltas were incomplete");
    }
    return {
      protocol: "after-all-first",
      warmOrdering: "after-all-first",
      firstPutBody: "x",
      step0FirstPutBody: "",
      concurrency,
      pairs,
      first,
      warm,
      deltas,
      valid: first.status === "success" && warm.status === "success" && deltas.status === "success",
    };
  });

export interface ThroughputOptions {
  readonly baseUrl: string;
  readonly trials?: number;
  readonly postsPerTrial?: number;
  readonly concurrency?: number;
}

export interface ThroughputTrial {
  readonly streamPath: string;
  readonly attempted: number;
  readonly elapsedMs?: number;
  readonly rate?: number;
  readonly errors: ReadonlyArray<string>;
}

export interface ThroughputMeasurement {
  readonly trials: ReadonlyArray<ThroughputTrial>;
  readonly medianRate?: number;
  readonly valid: boolean;
  readonly diagnostics: { readonly step0Rate: number; readonly priorReferenceRate: number };
}

export const measureThroughput = (
  options: ThroughputOptions,
): Effect.Effect<ThroughputMeasurement, ContractError, HttpOperation> =>
  Effect.gen(function* () {
    const trials = options.trials ?? 5;
    const postsPerTrial = options.postsPerTrial ?? 500;
    const concurrency = options.concurrency ?? 16;
    const results = yield* Effect.forEach(
      Array.from({ length: trials }, (_, trial) => trial),
      (trial) =>
        Effect.gen(function* () {
          const http = yield* HttpOperation;
          const streamPath = `/streams/throughput-${trial}-${crypto.randomUUID()}`;
          const errors: Array<string> = [];
          const created = yield* Effect.result(
            http.request({
              method: "PUT",
              url: new URL(streamPath, options.baseUrl).toString(),
              body: "",
              headers: { "content-type": "text/plain" },
            }),
          );
          if (Result.isFailure(created)) errors.push(`create: ${created.failure.message}`);
          else if (created.success.status < 200 || created.success.status >= 300)
            errors.push(`create: HTTP ${created.success.status}`);
          const start = yield* Clock.currentTimeNanos;
          const posts = yield* Effect.forEach(
            Array.from({ length: postsPerTrial }, (_, index) => index),
            () =>
              Effect.result(
                http.request({
                  method: "POST",
                  url: new URL(streamPath, options.baseUrl).toString(),
                  body: "x",
                  headers: { "content-type": "text/plain" },
                }),
              ),
            { concurrency },
          );
          const end = yield* Clock.currentTimeNanos;
          for (const [index, post] of posts.entries()) {
            if (Result.isFailure(post)) errors.push(`post ${index}: ${post.failure.message}`);
            else if (post.success.status < 200 || post.success.status >= 300)
              errors.push(`post ${index}: HTTP ${post.success.status}`);
          }
          const elapsedMs = Number(end - start) / 1_000_000;
          const rate =
            errors.length === 0 && elapsedMs > 0 ? postsPerTrial / (elapsedMs / 1_000) : undefined;
          return {
            streamPath,
            attempted: postsPerTrial,
            elapsedMs,
            rate,
            errors,
          } satisfies ThroughputTrial;
        }),
      { concurrency: 1 },
    );
    const validRates = results
      .map((trial) => trial.rate)
      .filter((rate): rate is number => rate !== undefined);
    return {
      trials: results,
      medianRate: validRates.length === results.length ? percentile(validRates, 50) : undefined,
      valid: validRates.length === results.length,
      diagnostics: { step0Rate: 54.60448413131066, priorReferenceRate: 91 },
    };
  });

export const measurementDefaults = {
  conformanceLongPollTimeoutMs: CONFORMANCE_LONG_POLL_TIMEOUT_MS,
} as const;
