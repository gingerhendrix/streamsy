import { Clock, Duration, Effect, Exit, Layer, Option, Pull, Schedule, Schema } from "effect";
import type { Storage, StorageFault } from "@streamsy/core";
import type { InputMap, InputServices } from "./batch.ts";
import type { Executable } from "./executable.ts";
import { runnerOf, type Attachable } from "./runner.ts";
import { Registry, registerRunner, type RunnerStatus } from "./registry.ts";
import { retryable } from "./failure.ts";
import { ProjectionRetry } from "./retry.ts";
import type { WakeOutput } from "./wake.ts";

export type { Attachable } from "./runner.ts";

export interface AttachOptions {
  /** Local wake streams whose source ids request binding runs. Use fixed-address wake classes. */
  readonly triggers?: ReadonlyArray<WakeOutput<{}>>;
  /** Units for each binding in one run. Default 100. */
  readonly limit?: number;
  /**
   * Timing only. The runner calls the schedule once per retryable failed pass with no input.
   * Paces retryable failures reading marks, cursors or triggers. Retryable binding
   * failures back off alone, 1 s doubling to 1 hour. Non-retryable faults stop their unit.
   * Defaults to capped, jittered exponential backoff.
   */
  readonly retry?: Schedule.Schedule<unknown>;
  /**
   * Keep a pending mark for each binding in the storage owner, written in the
   * transaction of the commit that needs the run. After a restart or an
   * eviction, the runner finds the marks and runs those bindings. Otherwise
   * pending work, binding backoff and stops live only for this runner.
   * Needs a local memory or SQL storage layer.
   *
   * Default: false for `Projection.attach`, true for `ProjectionWakeup.attach`.
   */
  readonly durable?: boolean;
  /** The due marks that one drain pass reads. Default 25. */
  readonly batch?: number;
  /** Bindings run at once. Default 1. Must be a positive safe integer. */
  readonly concurrency?: number;
}

/** A defect of one drain pass, captured for logging with typed failures. */
class PassDefect extends Schema.TaggedError<PassDefect, { readonly brand: unique symbol }>()(
  "PassDefect",
  {
    defect: Schema.Defect(),
  },
) {}

/**
 * Run a projection whenever a local commit writes one of its inputs. The layer
 * subscribes to `Storage.committed` when it is built, so build it with the
 * storage layer and before the app writes. For each written id, the input's
 * `parse` gives the params, and the runner runs `projection.at(params)`.
 *
 * Use it on long-lived hosts (Bun, Node). On a Cloudflare object, use
 * `ProjectionWakeup.attach(projection)`.
 *
 * By default the set of bindings with work lives in memory. A crash after a
 * commit and before the run leaves that binding late until its source gets its
 * next write. With `durable: true`, a pending mark in storage covers that
 * window, and the layer runs due marks when it starts.
 * Inputs on another owner need a local wake stream in `triggers`. Do not run
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
  const runner = runnerOf(projection, {
    limit: options.limit,
    durable: options.durable ?? false,
    batch: options.batch,
    concurrency: options.concurrency,
    triggers: options.triggers,
  });

  return Layer.effectDiscard(
    Effect.gen(function* () {
      const { wait, drain, status, schedule, retry, failed, interruptDelay, resume } =
        yield* runner;

      const policy = options.retry ?? (yield* ProjectionRetry);
      const step = yield* Schedule.toStep(policy);

      type Status = {
        state: RunnerStatus["state"];
        failures: number;
        retryAt: Option.Option<number>;
        lastPassAt: Option.Option<number>;
        step: typeof step;
      };

      const current: Status = {
        state: "running",
        failures: 0,
        retryAt: Option.none<number>(),
        lastPassAt: Option.none<number>(),
        step,
      };

      const registry = yield* Effect.serviceOption(Registry);

      const changed = Option.isNone(registry)
        ? Effect.void
        : yield* registerRunner(registry.value, {
            id: projection.id,
            version: projection.version,
            generation: projection.generation,
            schedule,
            retry,
            failed,
            status: status.pipe(
              Effect.map((runnerStatus): RunnerStatus => ({
                projection: projection.id,
                state: current.state,
                failures: current.failures,
                retryAt: current.retryAt,
                lastPassAt: current.lastPassAt,
                ...runnerStatus,
              })),
            ),
          });

      // A wait reads marks, so a failed read retries like a failed drain.
      yield* Effect.gen(function* () {
        let requested = false;

        while (true) {
          current.state = "running";
          current.retryAt = Option.none();

          const exit: Exit.Exit<{ readonly more: boolean }, StorageFault | PassDefect> =
            yield* Effect.exit(
              (requested ? Effect.void : wait).pipe(
                Effect.andThen(drain),
                Effect.catchDefect((defect) => Effect.fail(new PassDefect({ defect }))),
              ),
            );

          requested = false;

          current.lastPassAt = Option.some(yield* Clock.currentTimeMillis);

          if (Exit.isSuccess(exit)) {
            current.failures = 0;
            current.step = yield* Schedule.toStep(policy);
            yield* changed;
            continue;
          }

          current.failures += 1;
          yield* Effect.logError(`Projection.attach ${projection.id} failed`, exit.cause);

          const now = yield* Clock.currentTimeMillis;

          const delay: Option.Option<number> = yield* !retryable(exit.cause)
            ? Effect.succeedNone
            : Effect.suspend(() => current.step(now, undefined)).pipe(
                Effect.map(([, duration]) => Option.some(Duration.toMillis(duration))),
                Pull.catchDone(() => Effect.succeedNone),
                Effect.catchCause((cause) =>
                  Effect.logError(
                    `Projection.attach ${projection.id} retry schedule failed`,
                    cause,
                  ).pipe(Effect.as(Option.none<number>())),
                ),
              );

          if (Option.isSome(delay)) {
            current.state = "retrying";
            current.retryAt = Option.some((yield* Clock.currentTimeMillis) + delay.value);
            yield* changed;
            requested = yield* interruptDelay(delay.value);
          } else {
            current.state = "failed";
            yield* changed;
            // Exhaustion stops automatic retries; every request permits one more pass.
            requested = yield* resume;
          }
        }
      }).pipe(Effect.withSpan("Projection.attach"), Effect.forkScoped);
    }),
  );
};
