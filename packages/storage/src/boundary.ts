import {
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Queue,
  Schedule,
  Scope,
  Stream,
} from "effect";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

class PendingInvalidations extends Context.Service<PendingInvalidations, Set<string>>()(
  "@streamsy/storage/PendingInvalidations",
) {}

export interface CommitBoundaryApi {
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError, R>;
}

export class CommitBoundary extends Context.Service<CommitBoundary, CommitBoundaryApi>()(
  "@streamsy/storage/CommitBoundary",
) {}

interface ActiveSubscription {
  readonly queue: Queue.Queue<void>;
  readonly cleanup: Effect.Effect<void>;
}

/** Internal-only observability used by the official-driver conformance harnesses. */
export interface BoundaryTestProbe {
  activeRegistrations: number;
  activeRepairFibers: number;
  repairPasses: number;
  repairsPaused: boolean;
  closed: boolean;
  readonly queues: Set<Queue.Queue<void>>;
  afterRegister?: Effect.Effect<void>;
}

export const makeBoundaryTestProbe = (): BoundaryTestProbe => ({
  activeRegistrations: 0,
  activeRepairFibers: 0,
  repairPasses: 0,
  repairsPaused: false,
  closed: false,
  queues: new Set(),
});

const noop = () => {};

export interface BoundaryRuntime extends CommitBoundaryApi {
  readonly mutation: <A, E, R>(options: {
    readonly keys: ReadonlyArray<string>;
    readonly effect: Effect.Effect<A, E, R>;
    readonly committed: (value: A) => boolean;
  }) => Effect.Effect<A, E | SqlError, R>;
  readonly changes: <A, E, R>(options: {
    readonly keys: ReadonlyArray<string>;
    readonly read: Effect.Effect<A, E, R>;
  }) => Stream.Stream<A, E, R>;
}

export class BoundaryRuntimeService extends Context.Service<
  BoundaryRuntimeService,
  BoundaryRuntime
>()("@streamsy/storage/BoundaryRuntime") {}

export const sharedSqlClientLayer = <A extends SqlClient.SqlClient>(
  makeClient: Effect.Effect<A, never, Scope.Scope | Reactivity.Reactivity>,
) =>
  Context.empty().pipe(
    Effect.succeed,
    Effect.flatMap(() => Reactivity.make),
    Effect.flatMap((reactivity) =>
      makeClient.pipe(
        Effect.provideService(Reactivity.Reactivity, reactivity),
        Effect.map((client) =>
          Context.empty().pipe(
            Context.add(SqlClient.SqlClient, client),
            Context.add(Reactivity.Reactivity, reactivity),
          ),
        ),
      ),
    ),
  );

const makeBoundary = (
  repairIntervalMs: number,
  probe?: BoundaryTestProbe,
): Effect.Effect<
  BoundaryRuntime,
  never,
  Scope.Scope | SqlClient.SqlClient | Reactivity.Reactivity
> =>
  Effect.gen(function* () {
    if (!Number.isFinite(repairIntervalMs) || repairIntervalMs <= 0)
      return yield* Effect.die(new RangeError("repairIntervalMs must be positive"));
    const sql = yield* SqlClient.SqlClient;
    const reactivity = yield* Reactivity.Reactivity;
    const subscriptions = new Set<ActiveSubscription>();
    let closed = false;

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const active = yield* Effect.sync(() => {
          closed = true;
          if (probe !== undefined) probe.closed = true;
          return [...subscriptions];
        });
        for (const subscription of active) yield* subscription.cleanup;
      }),
    );

    const withTransaction: CommitBoundaryApi["withTransaction"] = (body) =>
      Effect.flatMap(Effect.serviceOption(PendingInvalidations), (owned) => {
        if (Option.isSome(owned)) return body;
        return Effect.flatMap(Effect.serviceOption(sql.transactionService), (ambient) => {
          if (Option.isSome(ambient))
            return Effect.die(
              new Error("Commit boundary cannot enter an unowned ambient SQL transaction"),
            );
          const pending = new Set<string>();
          return sql
            .withTransaction(body.pipe(Effect.provideService(PendingInvalidations, pending)))
            .pipe(
              Effect.tap(() =>
                pending.size === 0 ? Effect.void : reactivity.invalidate([...pending]),
              ),
            );
        });
      });

    const mutation: BoundaryRuntime["mutation"] = ({ keys, effect, committed }) =>
      Effect.flatMap(Effect.serviceOption(sql.transactionService), (ambient) =>
        Effect.flatMap(Effect.serviceOption(PendingInvalidations), (pendingBefore) => {
          // This check is deliberately before `effect`: a foreign raw ambient
          // transaction cannot execute an unnotified storage statement.
          if (Option.isSome(ambient) && Option.isNone(pendingBefore))
            return Effect.die(new Error("Ambient SQL transaction is not owned by CommitBoundary"));
          const run = Effect.tap(effect, (value) =>
            Effect.flatMap(Effect.serviceOption(PendingInvalidations), (pending) => {
              if (Option.isNone(pending))
                return Effect.die(new Error("CommitBoundary owner context disappeared"));
              return Effect.sync(() => {
                if (committed(value)) for (const key of keys) pending.value.add(key);
              });
            }),
          );
          return Option.isSome(ambient) ? run : withTransaction(run);
        }),
      );

    const changes: BoundaryRuntime["changes"] = ({ keys, read }) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* Effect.acquireRelease(
            Effect.gen(function* () {
              const queue = yield* Queue.make<void>({ capacity: 1, strategy: "dropping" });
              const repairStarted = yield* Deferred.make<void>();
              const repairFiber = yield* Effect.acquireUseRelease(
                Effect.sync(() => {
                  if (probe !== undefined) probe.activeRepairFibers += 1;
                }),
                () =>
                  Deferred.succeed(repairStarted, undefined).pipe(
                    Effect.andThen(Effect.sleep(repairIntervalMs)),
                    Effect.andThen(
                      Effect.suspend(() => {
                        if (probe?.repairsPaused === true) return Effect.void;
                        if (probe !== undefined) probe.repairPasses += 1;
                        return Queue.offer(queue, undefined);
                      }),
                    ),
                    Effect.repeat(Schedule.forever),
                  ),
                () =>
                  Effect.sync(() => {
                    if (probe !== undefined) probe.activeRepairFibers -= 1;
                  }),
              ).pipe(Effect.forkScoped);
              yield* Deferred.await(repairStarted);
              let cleaned = false;
              let cancel = noop;
              const active: ActiveSubscription = {
                queue,
                cleanup: Effect.suspend(() => {
                  if (cleaned) return Effect.void;
                  cleaned = true;
                  subscriptions.delete(active);
                  probe?.queues.delete(queue);
                  if (probe !== undefined) probe.activeRegistrations -= 1;
                  cancel();
                  return Fiber.interrupt(repairFiber).pipe(Effect.andThen(Queue.shutdown(queue)));
                }),
              };
              const registered = yield* Effect.sync(() => {
                if (closed) return false;
                cancel = reactivity.registerUnsafe(keys, () => Queue.offerUnsafe(queue, undefined));
                subscriptions.add(active);
                probe?.queues.add(queue);
                if (probe !== undefined) probe.activeRegistrations += 1;
                return true;
              });
              if (!registered) {
                yield* Fiber.interrupt(repairFiber);
                yield* Queue.shutdown(queue);
                return yield* Effect.interrupt;
              }
              if (probe?.afterRegister !== undefined) yield* probe.afterRegister;
              return active;
            }),
            (active) => active.cleanup,
          );
          return Stream.concat(
            Stream.fromEffect(read),
            Stream.fromQueue(subscription.queue).pipe(Stream.mapEffect(() => read)),
          );
        }),
      );

    return { withTransaction, mutation, changes };
  });

export const boundaryLayer = (repairIntervalMs: number, probe?: BoundaryTestProbe) =>
  Layer.effect(BoundaryRuntimeService, makeBoundary(repairIntervalMs, probe));
