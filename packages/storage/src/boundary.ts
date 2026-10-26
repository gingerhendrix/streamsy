import {
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Queue,
  PubSub,
  Schedule,
  Scheduler,
  Scope,
  Stream,
} from "effect";
import { Reactivity } from "effect/reactivity";
import { SqlClient, SqlError } from "effect/sql";
import type { CommitBoundaryApi, StreamId } from "@streamsy/core";
import { retryWithPolicy, type TransactionRetryPolicy } from "./transaction-retry.ts";

interface Pending {
  readonly keys: Set<string>;
  /** Stream ids that the transaction's mutations wrote, for `Storage.committed`. */
  readonly ids: Set<StreamId>;
}

class PendingInvalidations extends Context.Service<PendingInvalidations, Pending>()(
  "@streamsy/storage/PendingInvalidations",
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
  transactionAttempts: number;
  migrationAttempts: number;
  invalidations: number;
  completedMutations: number;
  readonly queues: Set<Queue.Queue<void>>;
  afterRegister?: Effect.Effect<void>;
}

export const makeBoundaryTestProbe = (): BoundaryTestProbe => ({
  activeRegistrations: 0,
  activeRepairFibers: 0,
  repairPasses: 0,
  repairsPaused: false,
  closed: false,
  transactionAttempts: 0,
  migrationAttempts: 0,
  invalidations: 0,
  completedMutations: 0,
  queues: new Set(),
});

const noop = () => {};

export interface BoundaryRuntime extends CommitBoundaryApi {
  readonly mutation: <A, E, R>(options: {
    readonly keys: ReadonlyArray<string>;
    readonly ids: ReadonlyArray<StreamId>;
    readonly effect: Effect.Effect<A, E, R>;
    readonly retryable: (error: E) => boolean;
  }) => Effect.Effect<A, E | SqlError.SqlError, R>;
  readonly changes: <A, E, R>(options: {
    readonly keys: ReadonlyArray<string>;
    readonly read: Effect.Effect<A, E, R>;
  }) => Stream.Stream<A, E, R>;
  /** `Storage.committed`: a subscription to the ids of each later owned commit. */
  readonly committed: Effect.Effect<
    PubSub.Subscription<ReadonlyArray<StreamId>>,
    never,
    Scope.Scope
  >;
}

export class BoundaryRuntimeService extends Context.Service<
  BoundaryRuntimeService,
  BoundaryRuntime
>()("@streamsy/storage/BoundaryRuntime") {}

export const sharedSqlClientLayer = <A extends SqlClient.SqlClient, E>(
  makeClient: Effect.Effect<A, E, Scope.Scope | Reactivity.Reactivity>,
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

/**
 * Build a `SqlClient` and provide it with the `Reactivity` it captured, so the
 * client and the storage boundary share one change graph.
 *
 * ```ts
 * const storage = Storage.layer(streams).pipe(
 *   Layer.provide(Storage.clientLayer(PgClient.make({ url: Redacted.make(url) }))),
 * );
 * ```
 */
export const clientLayer = <A extends SqlClient.SqlClient, E>(
  makeClient: Effect.Effect<A, E, Scope.Scope | Reactivity.Reactivity>,
): Layer.Layer<SqlClient.SqlClient | Reactivity.Reactivity, E> =>
  Layer.effectContext(sharedSqlClientLayer(makeClient));

const makeBoundary = (
  repairIntervalMs: number,
  transactionRetry: TransactionRetryPolicy,
  probe?: BoundaryTestProbe,
  transactionMaxOpsBeforeYield?: number,
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

    const feed = yield* Effect.acquireRelease(
      PubSub.unbounded<ReadonlyArray<StreamId>>(),
      PubSub.shutdown,
    );

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

    const ownedTransaction = <A, E, R>(body: Effect.Effect<A, E, R>) =>
      Effect.suspend(() => {
        if (probe !== undefined) probe.transactionAttempts += 1;
        const pending: Pending = { keys: new Set(), ids: new Set() };
        let bodyCompleted = false;

        const transactionBody = body.pipe(
          Effect.provideService(PendingInvalidations, pending),
          Effect.tap(() =>
            Effect.sync(() => {
              bodyCompleted = true;
            }),
          ),
        );

        // A SqlError after the body completed comes from COMMIT. Streamsy treats
        // a failed COMMIT as uncertain and never replays it. Effect 4.0.5 returns
        // it on the error channel, where a retry policy would replay it, so turn
        // it back into a defect, as Effect 4.0.1 did.
        const transaction = sql.withTransaction(transactionBody).pipe(
          Effect.catchIf(
            (error) => bodyCompleted && SqlError.isSqlError(error),
            (error) => Effect.die(error),
          ),
        );

        const guardedTransaction =
          transactionMaxOpsBeforeYield === undefined
            ? transaction
            : transaction.pipe(
                Effect.provideService(Scheduler.MaxOpsBeforeYield, transactionMaxOpsBeforeYield),
              );

        return guardedTransaction.pipe(
          Effect.tap(() => {
            if (pending.keys.size === 0) return Effect.void;

            if (probe !== undefined) probe.invalidations += 1;

            return reactivity.invalidate([...pending.keys]);
          }),
          Effect.tap(() =>
            pending.ids.size === 0 ? Effect.void : PubSub.publish(feed, [...pending.ids]),
          ),
        );
      });

    const withTransaction: CommitBoundaryApi["withTransaction"] = (body) =>
      Effect.flatMap(Effect.serviceOption(PendingInvalidations), (owned) => {
        if (Option.isSome(owned)) return body;

        return Effect.flatMap(Effect.serviceOption(sql.transactionService), (ambient) => {
          if (Option.isSome(ambient))
            return Effect.die(
              new Error("Commit boundary cannot enter an unowned ambient SQL transaction"),
            );

          return ownedTransaction(body);
        });
      });

    const mutation: BoundaryRuntime["mutation"] = ({ keys, ids, effect, retryable }) =>
      Effect.flatMap(Effect.serviceOption(sql.transactionService), (ambient) =>
        Effect.flatMap(Effect.serviceOption(PendingInvalidations), (pendingBefore) => {
          // This check is deliberately before `effect`: a foreign raw ambient
          // transaction cannot execute an unnotified storage statement.
          if (Option.isSome(ambient) && Option.isNone(pendingBefore))
            return Effect.die(new Error("Ambient SQL transaction is not owned by CommitBoundary"));

          const run = Effect.tap(effect, () =>
            Effect.flatMap(Effect.serviceOption(PendingInvalidations), (pending) => {
              if (Option.isNone(pending))
                return Effect.die(new Error("CommitBoundary owner context disappeared"));

              return Effect.sync(() => {
                // A mutation that succeeds is applied: a rejection travels on the error
                // channel and rolls the transaction back.
                for (const key of keys) pending.value.keys.add(key);

                for (const id of ids) pending.value.ids.add(id);
              });
            }),
          );

          return Option.isSome(ambient)
            ? run
            : retryWithPolicy(ownedTransaction(run), transactionRetry, (error) =>
                SqlError.isSqlError(error) ? error.isRetryable : retryable(error),
              );
        }),
      ).pipe(
        Effect.onExit(() =>
          Effect.sync(() => {
            if (probe !== undefined) probe.completedMutations += 1;
          }),
        ),
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

    return { withTransaction, mutation, changes, committed: PubSub.subscribe(feed) };
  });

export const boundaryLayer = (
  repairIntervalMs: number,
  transactionRetry: TransactionRetryPolicy,
  probe?: BoundaryTestProbe,
  transactionMaxOpsBeforeYield?: number,
) =>
  Layer.effect(
    BoundaryRuntimeService,
    makeBoundary(repairIntervalMs, transactionRetry, probe, transactionMaxOpsBeforeYield),
  );
