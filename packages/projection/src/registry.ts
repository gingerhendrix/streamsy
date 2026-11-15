import {
  Array,
  Context,
  Data,
  Effect,
  Layer,
  Option,
  Predicate,
  PubSub,
  Record,
  Result,
  Stream,
  type Scope,
} from "effect";
import { compare, type StorageFault } from "@streamsy/core";
import type { MarkFailure, ProjectionMarkSummary } from "@streamsy/core/internal";
import { read, type Readable } from "./snapshot.ts";
import type { InputMap } from "./batch.ts";
import { InvalidBinding, NotAttached, ProjectionFault } from "./fault.ts";
import type { Identity } from "./checkpoint-store.ts";
import type { Attachable } from "./runner.ts";
import type { Progress } from "./run.ts";

/** Only identity and encoded params are needed; execution uses the runner's binding. */
type Binding = Pick<Identity, "id" | "params">;

type Declaration = Omit<Attachable<Binding>, "retry">;

/** Last failure of a binding, trigger stream or runner pass. */
export interface RunnerFailure {
  readonly at: number;
  readonly retryable: boolean;
  readonly reason: Option.Option<ProjectionFault["reason"]>;
  readonly message: string;
}

/** Live binding observations with the binding's encoded params. */
export type RunnerEvent = Data.TaggedEnum<{
  Ran: {
    readonly projection: string;
    readonly params: Readonly<Record<string, string>>;
    readonly status: Progress<unknown>["status"];
    readonly units: number;
    readonly offsets: Readonly<Record<string, string>>;
    /** Next recorded process wake after this run. */
    readonly wakeAt: Option.Option<number>;
  };
  Failed: {
    readonly projection: string;
    readonly params: Readonly<Record<string, string>>;
    readonly attempts: number;
    /** Binding backoff deadline; None while stopped. */
    readonly dueAt: Option.Option<number>;
    readonly failure: RunnerFailure;
    readonly stopped: boolean;
  };
  Dropped: {
    readonly projection: string;
    readonly params: Readonly<Record<string, string>>;
  };
}>;

export const RunnerEvent = Data.taggedEnum<RunnerEvent>();

/** One live, attached runner. Process fields reset when the host restarts. */
export interface RunnerStatus {
  readonly projection: string;
  readonly state: "running" | "retrying" | "failed";
  /** Consecutive failed passes, rather than failed bindings. */
  readonly failures: number;
  /** Set while waiting for an automatic or explicitly requested pass retry. */
  readonly retryAt: Option.Option<number>;
  readonly active: number;
  readonly queued: number;
  readonly lastPassAt: Option.Option<number>;
  /** Binding marks in either durability mode. The five counts are disjoint. */
  readonly marks: ProjectionMarkSummary;
  /** Last pass failure, cleared by a successful drain. */
  readonly failure: Option.Option<RunnerFailure>;
  readonly stopped: number;
  readonly triggers: ReadonlyArray<{
    readonly stream: string;
    readonly failure: Option.Option<RunnerFailure>;
  }>;
}

export interface RegistryApi {
  readonly status: Effect.Effect<ReadonlyArray<RunnerStatus>, StorageFault>;
  /** Full snapshots after subscription and changes; failed reads are logged and skipped. */
  readonly changes: Stream.Stream<ReadonlyArray<RunnerStatus>>;
  /** Future events only. Unbounded buffering keeps slow consumers off the runner's path. */
  readonly events: Stream.Stream<RunnerEvent>;
  /** Queue this binding now, overriding backoff and a stop. Invalid encoded params fail with InvalidBinding. */
  readonly schedule: (
    binding: Binding,
  ) => Effect.Effect<void, InvalidBinding | NotAttached | StorageFault>;
  /**
   * Wait for the attached identity's checkpoint, subscribing before the first read.
   * Matching failed runs prompt a reread for committed progress. No timeout.
   */
  readonly awaitOffset: <Inputs extends InputMap, S = never, Q = never>(
    binding: Readable<Inputs, S, Q>,
    input: keyof Inputs & string,
    offset: string,
  ) => Effect.Effect<void, NotAttached | ProjectionFault, Q>;
  /** Stored failed bindings, stopped first, then by deadline. Default limit 100, maximum 1,000. */
  readonly failed: (
    target: string | Declaration | Binding,
    options?: { readonly limit?: number },
  ) => Effect.Effect<
    ReadonlyArray<{
      readonly params: Readonly<Record<string, string>>;
      readonly attempts: number;
      readonly dueAt: Option.Option<number>;
      readonly stopped: boolean;
      /** Stopped and the binding retry budget or schedule has ended. */
      readonly exhausted: boolean;
      readonly failure: Option.Option<MarkFailure>;
    }>,
    NotAttached | StorageFault
  >;
  /** Make failed and stopped work due now; a projection target also resumes its triggers. Returns the number of failed bindings selected. */
  readonly retry: (
    target: string | Declaration | Binding,
  ) => Effect.Effect<number, NotAttached | StorageFault>;
}

interface RegisteredRunner {
  readonly id: string;
  readonly version: number;
  readonly generation: number;
  readonly status: Effect.Effect<RunnerStatus, StorageFault>;
  readonly schedule: (binding: Binding) => Effect.Effect<void, InvalidBinding | StorageFault>;
  readonly retry: (binding?: Binding) => Effect.Effect<number, StorageFault>;
  readonly failed: (limit: number, binding?: Binding) => ReturnType<RegistryApi["failed"]>;
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
  /** Match a binding's projection id and encoded params, independent of key order. */
  static readonly isFor =
    (binding: Binding) =>
    (event: RunnerEvent): boolean =>
      event.projection === binding.id &&
      Record.size(event.params) === Record.size(binding.params) &&
      Array.every(Record.toEntries(binding.params), ([key, value]) => event.params[key] === value);

  /** Provide below the attach layers so they see this same registry. */
  static readonly layer: Layer.Layer<Registry> = Layer.effect(
    this,
    Effect.gen(function* () {
      const runners = new Map<string, RegisteredRunner>();
      let registered = false;

      const updates = yield* Effect.acquireRelease(PubSub.sliding<void>(1), PubSub.shutdown);

      const events = yield* Effect.acquireRelease(PubSub.unbounded<RunnerEvent>(), PubSub.shutdown);

      const status = Effect.suspend(() =>
        Effect.forEach(runners.values(), (runner) => runner.status),
      );

      const find = (target: string | Declaration | Binding) =>
        Effect.suspend(() => {
          const id = Predicate.isString(target) ? target : target.id;
          const found = Option.fromUndefinedOr(runners.get(id));

          return Effect.fromOption(found).pipe(
            Effect.mapError(() => new NotAttached({ projection: id })),
          );
        });

      // Snapshot reads belong to subscribers, never to the projection pass.
      const publish = PubSub.publish(updates, undefined).pipe(Effect.asVoid);

      const snapshot = status.pipe(
        Effect.asSome,
        Effect.catch((fault) =>
          Effect.logWarning("Projection.Registry changes snapshot failed", fault).pipe(
            Effect.as(Option.none<ReadonlyArray<RunnerStatus>>()),
          ),
        ),
      );

      // Eager acquisition matters: a run between subscription and the first pull is retained.
      const observations = Stream.fromPull(
        Effect.map(PubSub.subscribe(events), PubSub.takeAll),
      ).pipe(Stream.scoped);

      const registry = Registry.of({
        status,
        events: observations,
        awaitOffset: Effect.fn("Projection.Registry.awaitOffset")(function* <
          Inputs extends InputMap,
          S = never,
          Q = never,
        >(binding: Readable<Inputs, S, Q>, input: keyof Inputs & string, offset: string) {
          const reached = (value: string | undefined) =>
            value !== undefined && compare(value, offset) >= 0;

          return yield* Effect.scoped(
            Effect.gen(function* () {
              const runner = yield* find(binding);

              if (runner.version !== binding.version || runner.generation !== binding.generation)
                return yield* new ProjectionFault({
                  phase: "load",
                  reason: "identity-mismatch",
                  message: `Attached identity of ${binding.id} is version ${runner.version}, generation ${runner.generation}; binding is version ${binding.version}, generation ${binding.generation}`,
                });

              const pull = yield* Stream.toPull(observations);
              const checkpoint = yield* read(binding);

              if (reached(checkpoint.offsets[input])) return yield* Effect.void;

              return yield* Stream.fromPull(Effect.succeed(pull)).pipe(
                Stream.filter(Registry.isFor(binding)),
                Stream.mapEffect(
                  RunnerEvent.$match({
                    Ran: (event) => Effect.succeed(reached(event.offsets[input])),
                    Failed: () =>
                      read(binding).pipe(Effect.map((value) => reached(value.offsets[input]))),
                    Dropped: () => Effect.succeed(false),
                  }),
                ),
                Stream.filter((satisfied) => satisfied),
                Stream.runHead,
                Effect.flatMap((event) => (Option.isSome(event) ? Effect.void : Effect.never)),
              );
            }),
          );
        }),
        schedule: Effect.fn("Projection.Registry.schedule")(function* (binding: Binding) {
          const runner = yield* find(binding);
          yield* runner.schedule(binding);
        }),
        failed: Effect.fn("Projection.Registry.failed")(function* (
          target: string | Declaration | Binding,
          options: { readonly limit?: number } = {},
        ) {
          const limit = options.limit ?? 100;

          if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000)
            return yield* Effect.die(
              new RangeError("failed limit must be an integer from 1 to 1,000"),
            );
          const runner = yield* find(target);

          return yield* runner.failed(
            limit,
            Predicate.isString(target) || "at" in target ? undefined : target,
          );
        }),
        retry: Effect.fn("Projection.Registry.retry")(function* (
          target: string | Declaration | Binding,
        ) {
          const runner = yield* find(target);

          return yield* runner.retry(
            Predicate.isString(target) || "at" in target ? undefined : target,
          );
        }),
        changes: Stream.unwrap(
          Effect.gen(function* () {
            // Subscribe before the initial read, so a pass cannot fall into a gap.
            const subscription = yield* PubSub.subscribe(updates);

            return Stream.concat(
              Stream.fromEffect(snapshot),
              Stream.fromSubscription(subscription).pipe(Stream.mapEffect(() => snapshot)),
            ).pipe(Stream.filterMap((value) => Result.fromOption(value, () => undefined)));
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

              if (runners.has(runner.id))
                throw new RangeError(
                  `Projection ${runner.id} is already attached to this registry`,
                );
              runners.set(runner.id, runner);
            }).pipe(Effect.andThen(publish)),
            () =>
              Effect.sync(() => {
                runners.delete(runner.id);
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

/** @internal The event publisher installed by Registry.layer. */
export const eventPublisher = (registry: RegistryApi) => registrations.get(registry)?.event;

/** @internal Register in the attach scope and return its pass notification. */
export const registerRunner = (
  registry: RegistryApi,
  runner: RegisteredRunner,
): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> => {
  const registration = registrations.get(registry);

  return registration === undefined
    ? Effect.die(
        new Error(
          "Projection runners require Registry.layer; a hand-built Registry has no registration support",
        ),
      )
    : registration.register(runner).pipe(Effect.as(registration.publish));
};
