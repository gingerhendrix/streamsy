import { Array, Context, Data, Effect, Layer, Option, PubSub, Stream, type Scope } from "effect";
import type { StorageFault } from "@streamsy/core";
import type { ProjectionMarkSummary } from "@streamsy/core/internal";
import { declarationOf } from "./declaration.ts";
import { NotAttached } from "./fault.ts";
import type { Identity } from "./checkpoint-store.ts";
import type { Attachable } from "./runner.ts";
import type { Progress } from "./run.ts";

/** Only identity and encoded params are needed; execution uses the runner's binding. */
type Binding = Pick<Identity, "id" | "params">;

type Declaration = Attachable<Binding>;

/** Live binding observations; params is the canonical JSON of encoded params. */
export type RunnerEvent =
  | {
      readonly _tag: "Ran";
      readonly projection: string;
      readonly params: string;
      readonly status: Progress<unknown>["status"];
      readonly units: number;
      readonly offsets: Readonly<Record<string, string>>;
    }
  | {
      readonly _tag: "Failed";
      readonly projection: string;
      readonly params: string;
      readonly attempts: number;
      /** None when the pass retry policy is exhausted. */
      readonly dueAt: Option.Option<number>;
    }
  | { readonly _tag: "Dropped"; readonly projection: string; readonly params: string };

export const RunnerEvent = Data.taggedEnum<RunnerEvent>();

/** One live, attached runner. Process fields reset when the host restarts. */
export interface RunnerStatus {
  readonly projection: string;
  readonly state: "running" | "retrying" | "failed";
  /** Consecutive failed passes, rather than failed durable bindings. */
  readonly failures: number;
  /** Set while waiting for an automatic or explicitly requested pass retry. */
  readonly retryAt: Option.Option<number>;
  readonly active: number;
  readonly queued: number;
  readonly lastPassAt: Option.Option<number>;
  /** None for a non-durable runner. The three counts are disjoint. */
  readonly marks: Option.Option<ProjectionMarkSummary>;
}

export interface RegistryApi {
  readonly status: Effect.Effect<ReadonlyArray<RunnerStatus>, StorageFault>;
  /** Full snapshots after subscription and changes; failed reads are logged and skipped. */
  readonly changes: Stream.Stream<ReadonlyArray<RunnerStatus>>;
  /** Future events only. Unbounded buffering keeps slow consumers off the runner's path. */
  readonly events: Stream.Stream<RunnerEvent>;
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
  readonly event: (event: RunnerEvent) => Effect.Effect<void>;
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

      const updates = yield* Effect.acquireRelease(PubSub.unbounded<void>(), PubSub.shutdown);

      const events = yield* Effect.acquireRelease(PubSub.unbounded<RunnerEvent>(), PubSub.shutdown);

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

      // Snapshot reads belong to subscribers, never to the projection pass.
      const publish = PubSub.publish(updates, undefined).pipe(Effect.asVoid);

      const snapshot = status.pipe(
        Effect.map(Option.some),
        Effect.catch((fault) =>
          Effect.logWarning("Projection.Registry changes snapshot failed", fault).pipe(
            Effect.as(Option.none<ReadonlyArray<RunnerStatus>>()),
          ),
        ),
      );

      const registry = Registry.of({
        status,
        // Acquire eagerly with toPull, so callers can subscribe before reading a checkpoint.
        events: Stream.fromPull(Effect.map(PubSub.subscribe(events), PubSub.takeAll)).pipe(
          Stream.scoped,
        ),
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
              Stream.fromEffect(snapshot),
              Stream.fromSubscription(subscription).pipe(Stream.mapEffect(() => snapshot)),
            ).pipe(
              Stream.filter(Option.isSome),
              Stream.map((value) => value.value),
            );
          }),
        ),
      });

      registrations.set(registry, {
        publish,
        event: (event) => PubSub.publish(events, event).pipe(Effect.asVoid),
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

/** @internal Absent for runners without the optional registry; no events need constructing. */
export const eventPublisher = (registry: RegistryApi) => registrations.get(registry)?.event;

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
