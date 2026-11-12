import { Array, Context, Effect, Exit, Layer, Option, PubSub, Stream, type Scope } from "effect";
import type { StorageFault } from "@streamsy/core";
import type { ProjectionMarkSummary } from "@streamsy/core/internal";
import { declarationOf } from "./declaration.ts";
import { NotAttached } from "./fault.ts";
import type { Identity } from "./checkpoint-store.ts";
import type { Attachable } from "./runner.ts";

/** Only identity and encoded params are needed; execution uses the runner's binding. */
type Binding = Pick<Identity, "id" | "params">;

type Declaration = Attachable<Binding>;

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
  /** Queue this binding and wake its runner, without another source commit. */
  readonly schedule: (binding: Binding) => Effect.Effect<void, NotAttached | StorageFault>;
  /** Make failed work due now. Returns the number of failed bindings changed. */
  readonly retry: (
    target: Declaration | Binding,
  ) => Effect.Effect<number, NotAttached | StorageFault>;
}

interface RegisteredRunner {
  readonly declaration: object;
  readonly status: Effect.Effect<RunnerStatus, StorageFault>;
  readonly schedule: (binding: Binding) => Effect.Effect<void, StorageFault>;
  readonly retry: (binding?: Binding) => Effect.Effect<number, StorageFault>;
}

interface Registration {
  readonly register: (runner: RegisteredRunner) => Effect.Effect<void, never, Scope.Scope>;
  readonly publish: Effect.Effect<void>;
}

// Registration stays private to the host adapters.
const registrations = new WeakMap<RegistryApi, Registration>();

/** Optional in-process view of Projection.attach runners. */
export class Registry extends Context.Service<Registry, RegistryApi>()(
  "@streamsy/projection/Registry",
) {
  /** Provide below the attach layers so they see this same registry. */
  static readonly layer: Layer.Layer<Registry> = Layer.effect(
    this,
    Effect.gen(function* () {
      const runners = new Set<RegisteredRunner>();
      let registered = false;

      const updates = yield* Effect.acquireRelease(
        PubSub.unbounded<Exit.Exit<ReadonlyArray<RunnerStatus>, StorageFault>>(),
        PubSub.shutdown,
      );

      const status = Effect.suspend(() => Effect.forEach(runners, (runner) => runner.status));

      const find = (target: Declaration | Binding) =>
        Effect.suspend(() => {
          const declaration = "at" in target ? target.at : declarationOf(target);

          const found = Array.findFirst(
            [...runners],
            (runner) => runner.declaration === declaration,
          );

          return Effect.fromOption(found).pipe(
            Effect.mapError(() => new NotAttached({ projection: target.id })),
          );
        });

      // A failed summary read belongs to observers, never to the projection pass.
      const publish = status.pipe(
        Effect.exit,
        Effect.flatMap((exit) => PubSub.publish(updates, exit)),
        Effect.asVoid,
      );

      const registry = Registry.of({
        status,
        schedule: (binding) => Effect.flatMap(find(binding), (runner) => runner.schedule(binding)),
        retry: (target) =>
          Effect.flatMap(find(target), (runner) =>
            runner.retry("at" in target ? undefined : target),
          ),
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
        register: (runner) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              registered = true;
              runners.add(runner);
            }).pipe(Effect.andThen(publish)),
            () =>
              Effect.sync(() => {
                runners.delete(runner);
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
  runner: RegisteredRunner,
): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> => {
  const registration = registrations.get(registry);

  return registration === undefined
    ? Effect.succeed(Effect.void)
    : registration.register(runner).pipe(Effect.as(registration.publish));
};
