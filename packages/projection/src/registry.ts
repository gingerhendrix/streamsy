import { Context, Effect, Exit, Layer, Option, PubSub, Stream, type Scope } from "effect";
import type { StorageFault } from "@streamsy/core";
import type { ProjectionMarkSummary } from "@streamsy/core/internal";

/** One live, attached runner. Process fields reset when the host restarts. */
export interface RunnerStatus {
  readonly projection: string;
  readonly state: "running" | "retrying" | "failed";
  /** Consecutive failed passes, rather than failed durable bindings. */
  readonly failures: number;
  /** Set only while waiting for the pass retry schedule. */
  readonly retryAt: Option.Option<number>;
  readonly active: number;
  readonly queued: number;
  readonly lastPassAt: Option.Option<number>;
  /** None for a non-durable runner. The three counts are disjoint. */
  readonly marks: Option.Option<ProjectionMarkSummary>;
}

export interface RegistryApi {
  readonly status: Effect.Effect<ReadonlyArray<RunnerStatus>, StorageFault>;
  /** An initial snapshot, then a full snapshot after each pass and lifecycle change. */
  readonly changes: Stream.Stream<ReadonlyArray<RunnerStatus>, StorageFault>;
}

interface Registration {
  readonly register: (
    status: Effect.Effect<RunnerStatus, StorageFault>,
  ) => Effect.Effect<void, never, Scope.Scope>;
  readonly publish: Effect.Effect<void>;
}

// Registration stays private to attach; the service exposes only observations.
const registrations = new WeakMap<RegistryApi, Registration>();

/** Optional in-process view of Projection.attach runners. */
export class Registry extends Context.Service<Registry, RegistryApi>()(
  "@streamsy/projection/Registry",
) {
  /** Provide below the attach layers so they see this same registry. */
  static readonly layer: Layer.Layer<Registry> = Layer.effect(
    this,
    Effect.gen(function* () {
      const runners = new Set<Effect.Effect<RunnerStatus, StorageFault>>();
      let registered = false;

      const updates = yield* Effect.acquireRelease(
        PubSub.unbounded<Exit.Exit<ReadonlyArray<RunnerStatus>, StorageFault>>(),
        PubSub.shutdown,
      );

      const status = Effect.suspend(() => Effect.all([...runners]));

      // A failed summary read belongs to observers, never to the projection pass.
      const publish = status.pipe(
        Effect.exit,
        Effect.flatMap((exit) => PubSub.publish(updates, exit)),
        Effect.asVoid,
      );

      const registry = Registry.of({
        status,
        changes: Stream.unwrap(
          Effect.gen(function* () {
            // Subscribe before the initial read, so a pass cannot fall into a gap.
            const subscription = yield* PubSub.subscribe(updates);

            return Stream.concat(
              Stream.fromEffect(status),
              Stream.fromSubscription(subscription).pipe(
                Stream.mapEffect((exit) =>
                  Exit.isSuccess(exit) ? Effect.succeed(exit.value) : Effect.failCause(exit.cause),
                ),
              ),
            );
          }),
        ),
      });

      registrations.set(registry, {
        publish,
        register: (read) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              registered = true;
              runners.add(read);
            }).pipe(Effect.andThen(publish)),
            () =>
              Effect.sync(() => {
                runners.delete(read);
              }).pipe(Effect.andThen(publish)),
          ).pipe(Effect.asVoid),
      });
      yield* Effect.addFinalizer(() =>
        registered
          ? Effect.void
          : Effect.logDebug(
              "Projection.Registry closed without any runner registrations; provide Registry.layer below the attach layers",
            ),
      );

      return registry;
    }),
  );
}

/** @internal Register in the attach scope and return its pass notification. */
export const registerRunner = (
  registry: RegistryApi,
  status: Effect.Effect<RunnerStatus, StorageFault>,
): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> => {
  const registration = registrations.get(registry);

  return registration === undefined
    ? Effect.succeed(Effect.void)
    : registration.register(status).pipe(Effect.as(registration.publish));
};
