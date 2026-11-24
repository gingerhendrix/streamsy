import { Clock, Effect, Option } from "effect";
import type { InputMap } from "./batch.ts";
import type { DecodedParams } from "./declaration.ts";
import type { Executable } from "./executable.ts";
import { runnerOf, type Attachable } from "./runner.ts";
import { retryable } from "./failure.ts";
import { ProjectionRetry } from "./retry.ts";
import type { AttachOptions } from "./attach.ts";
import { Registry, registerRunner, type RunnerStatus } from "./registry.ts";

// Structural host contract: projection never imports the serve package.
interface WakeupHost {
  readonly reconcile: Effect.Effect<void>;
  readonly retry: Effect.Effect<void>;
  readonly state: Effect.Effect<{
    readonly failures: number;
    readonly retryAt: Option.Option<number>;
  }>;
}

/**
 * Local commits choose bindings; each alarm drains the same runner used by
 * Projection.attach. Acquire it in the object layer's scope, before any
 * request writes. Triggers must belong to this storage owner. A provided trigger
 * service on another owner dies at attach with a RangeError naming its stream;
 * remote trigger followers require the process host, Projection.attach.
 * On acquire, startup replays from the checkpoint inventory or `options.bindings`
 * are queued for the alarm on this object.
 *
 * Durable by default: each commit writes a pending mark for its bindings in
 * the same transaction, so an eviction or a deploy between the commit and the
 * alarm loses no work. The host's acquire reconcile reads the marks and arms
 * the alarm, including recorded process timer deadlines. A start makes stopped marks due once; a still-broken binding
 * stops again. Stopped marks alone arm no alarm. Pass `durable: false` for private memory marks and timers that reset on eviction until the binding's next input.
 * A wake during backoff waits; a stopped timer fires late after retry or restart.
 * Return a time after unit.due to avoid repeatedly running a passed deadline.
 */
const attach = <
  I extends InputMap,
  E,
  R,
  Q,
  Result,
  P,
  OR = never,
  HE = never,
  HR = never,
  Params extends DecodedParams = DecodedParams,
  BE = never,
  BR = never,
>(
  projection: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>, HE, HR, Params>,
  options: AttachOptions<NoInfer<Params>, BE, BR> = {},
) => {
  const runner = runnerOf(projection, options, true, "wakeup");

  return Effect.gen(function* () {
    const { next, drain, deferred, status, schedule, rescan, retry, failed } = yield* runner;
    let host: WakeupHost | undefined;
    let running = false;
    let lastPassAt = Option.none<number>();
    const registry = yield* Effect.serviceOption(Registry);

    const changed: Effect.Effect<void> = Option.isNone(registry)
      ? Effect.void
      : yield* registerRunner(registry.value, {
          id: projection.id,
          generation: projection.generation,
          status: Effect.gen(function* () {
            const state =
              host === undefined
                ? { failures: 0, retryAt: Option.none<number>() }
                : yield* host.state;

            return {
              projection: projection.id,
              state: running
                ? "running"
                : Option.isSome(state.retryAt)
                  ? "retrying"
                  : state.failures > 0
                    ? "failed"
                    : "running",
              failures: state.failures,
              retryAt: running ? Option.none<number>() : state.retryAt,
              lastPassAt,
              ...(yield* status),
            } satisfies RunnerStatus;
          }),
          failed,
          schedule: (binding) =>
            schedule(binding).pipe(Effect.andThen(request), Effect.uninterruptible),
          rescan: (stream) => rescan(stream).pipe(Effect.andThen(request), Effect.uninterruptible),
          retry: (binding) =>
            retry(binding).pipe(
              Effect.tap(() => request),
              Effect.uninterruptible,
            ),
        });

    const request = Effect.gen(function* () {
      if (host === undefined) return;
      yield* host.retry;
      yield* host.reconcile;
      yield* changed;
    });

    return {
      name: `streamsy/projection/${encodeURIComponent(projection.id)}`,
      next,
      retryable,
      run: Effect.suspend(() => {
        running = true;

        return drain.pipe(
          Effect.ensuring(
            Clock.currentTimeMillis.pipe(
              Effect.map((at) => {
                running = false;
                lastPassAt = Option.some(at);
              }),
            ),
          ),
        );
      }),
      bind: (bound: WakeupHost) =>
        Effect.sync(() => {
          host = bound;
        }),
      changed,
      retry: options.passRetry ?? (yield* ProjectionRetry),
      deferred,
    };
  });
};

export const ProjectionWakeup = { attach };
