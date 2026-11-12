import { Clock, Effect, Option } from "effect";
import type { InputMap } from "./batch.ts";
import type { Executable } from "./executable.ts";
import { runnerOf, type Attachable } from "./runner.ts";
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
    /** Storage or deadline-read failure prevents explicit retry from making work due. */
    readonly retryBlocked?: boolean;
  }>;
}

/**
 * Local commits choose bindings; each alarm drains the same runner used by
 * Projection.attach. Acquire it in the object layer's scope, before any
 * request writes.
 *
 * Durable by default: each commit writes a pending mark for its bindings in
 * the same transaction, so an eviction or a deploy between the commit and the
 * alarm loses no work. The host's acquire reconcile reads the marks and arms
 * the alarm. Pass `durable: false` for the in-memory set only.
 */
const attach = <I extends InputMap, E, R, Q, Result, P, OR = never>(
  projection: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>>,
  options: AttachOptions = {},
) => {
  const runner = runnerOf(projection, {
    limit: options.limit,
    durable: options.durable ?? true,
    batch: options.batch,
  });

  return Effect.gen(function* () {
    const { next, drain, deferred, status, schedule, retry, reportFailures } = yield* runner;
    let host: WakeupHost | undefined;
    let running = false;
    let lastPassAt = Option.none<number>();
    const registry = yield* Effect.serviceOption(Registry);

    const changed: Effect.Effect<void> = Option.isNone(registry)
      ? Effect.void
      : yield* registerRunner(registry.value, {
          declaration: projection.at,
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
          schedule: (binding) =>
            schedule(binding).pipe(
              Effect.andThen(Effect.suspend(() => host?.retry ?? Effect.void)),
              Effect.andThen(Effect.suspend(() => host?.reconcile ?? Effect.void)),
              Effect.andThen(Effect.suspend(() => changed)),
              Effect.uninterruptible,
            ),
          retry: (binding) =>
            Effect.gen(function* () {
              // Check before changing marks or recording in-memory retry requests.
              if (host !== undefined && (yield* host.state).retryBlocked) return 0;
              const count = yield* retry(binding);

              if (count > 0 && host !== undefined) yield* host.retry;

              if (host !== undefined) yield* host.reconcile;

              if (count > 0) yield* changed;

              return count;
            }).pipe(Effect.uninterruptible),
        });

    return {
      name: `streamsy/projection/${encodeURIComponent(projection.id)}`,
      next,
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
      changed: Option.isNone(registry)
        ? Effect.void
        : Effect.gen(function* () {
            const retryAt =
              host === undefined ? Option.none<number>() : (yield* host.state).retryAt;

            yield* reportFailures(retryAt);
            yield* changed;
          }),
      retry: options.retry ?? (yield* ProjectionRetry),
      deferred,
    };
  });
};

export const ProjectionWakeup = { attach };
