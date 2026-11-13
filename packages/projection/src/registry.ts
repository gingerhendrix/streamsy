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
import type { ProjectionMarkSummary } from "@streamsy/core/internal";
import { read, type Readable } from "./snapshot.ts";
import type { InputMap } from "./batch.ts";
import { NotAttached, type ProjectionFault } from "./fault.ts";
import type { Identity } from "./checkpoint-store.ts";
import type { Attachable } from "./runner.ts";
import type { Progress } from "./run.ts";

/** Only identity and encoded params are needed; execution uses the runner's binding. */
type Binding = Pick<Identity, "id" | "params">;

type Declaration = Attachable<Binding>;

/** Live binding observations with the binding's encoded params. */
export type RunnerEvent = Data.TaggedEnum<{
  Ran: {
    readonly projection: string;
    readonly params: Readonly<Record<string, string>>;
    readonly status: Progress<unknown>["status"];
    readonly units: number;
    readonly offsets: Readonly<Record<string, string>>;
  };
  Failed: {
    readonly projection: string;
    readonly params: Readonly<Record<string, string>>;
    readonly attempts: number;
    /** Durable binding deadline; None for non-durable bindings, which retry with their pass. */
    readonly dueAt: Option.Option<number>;
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
  /** Wait for an attached binding's checkpoint, subscribing before the first read. No timeout. */
  readonly awaitOffset: <Inputs extends InputMap, S = never, Q = never>(
    binding: Readable<Inputs, S, Q>,
    input: keyof Inputs & string,
    offset: string,
  ) => Effect.Effect<void, NotAttached | ProjectionFault, Q>;
  /** Make failed work due now. Returns the number of failed bindings selected. */
  readonly retry: (
    target: string | Declaration | Binding,
  ) => Effect.Effect<number, NotAttached | StorageFault>;
}

interface RegisteredRunner {
  readonly id: string;
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
              yield* find(binding);
              const pull = yield* Stream.toPull(observations);
              const checkpoint = yield* read(binding);

              if (reached(checkpoint.offsets[input])) return;

              yield* Stream.fromPull(Effect.succeed(pull)).pipe(
                Stream.filter(Registry.isFor(binding)),
                Stream.filter(RunnerEvent.$is("Ran")),
                Stream.filter((event) => reached(event.offsets[input])),
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
