import { Effect, Layer, Schedule } from "effect";
import type { Storage } from "@streamsy/core";
import type { InputMap, InputServices } from "./batch.ts";
import type { Executable } from "./executable.ts";
import { runnerOf, type Attachable } from "./runner.ts";
import { ProjectionRetry } from "./retry.ts";

export type { Attachable } from "./runner.ts";

export interface AttachOptions {
  /** Units for each binding in one run. Default 100. */
  readonly limit?: number;
  /** Retry a failed drain pass. Defaults to capped, jittered exponential backoff. */
  readonly retry?: Schedule.Schedule<unknown>;
}

/**
 * Run a projection whenever a local commit writes one of its inputs. The layer
 * subscribes to `Storage.committed` when it is built, so build it with the
 * storage layer and before the app writes. For each written id, the input's
 * `parse` gives the params, and the runner runs `projection.at(params)`.
 *
 * Use it on long-lived hosts (Bun, Node). On a Cloudflare object, use
 * `ProjectionWakeup.attach(projection)`.
 *
 * The set of bindings with work lives in memory. A crash after a commit and
 * before the run leaves that binding late until its source gets its next write.
 * Inputs on another owner never commit here, so they never trigger. Do not run
 * an attached binding under `Projection.follow` too: use one writer per key.
 *
 * ```ts
 * const app = Layer.mergeAll(Api, Projection.attach(filesWake)).pipe(
 *   Layer.provideMerge(BunStorage.layer(streams, { path })),
 * );
 * ```
 */
export const attach = <I extends InputMap, E, R, Q, Result, P, OR = never>(
  projection: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>>,
  options: AttachOptions = {},
): Layer.Layer<never, never, Storage | Exclude<R, P> | Q | OR | InputServices<I>> => {
  const runner = runnerOf(projection, options.limit);

  return Layer.effectDiscard(
    Effect.gen(function* () {
      const { wait, drain } = yield* runner;
      const retry = options.retry ?? (yield* ProjectionRetry);
      yield* wait.pipe(
        Effect.andThen(
          drain.pipe(
            Effect.tapError((error) =>
              Effect.logError(`Projection.attach ${projection.id} failed`, error),
            ),
            Effect.retry(retry),
          ),
        ),
        Effect.repeat(Schedule.forever),
        Effect.withSpan("Projection.attach"),
        Effect.forkScoped,
      );
    }),
  );
};
