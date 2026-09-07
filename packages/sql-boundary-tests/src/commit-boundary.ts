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

class PendingInvalidations extends Context.Service<PendingInvalidations, Set<string>>()(
  "@streamsy/sql-boundary-proof/PendingInvalidations",
) {}

export interface CommitBoundaryDiagnostics {
  readonly activeSubscribers: number;
  readonly activeRegistrations: number;
  readonly activeRepairFibers: number;
  readonly retainedWakes: ReadonlyArray<number>;
  readonly repairPasses: number;
  readonly closed: boolean;
}

export interface CommitBoundary {
  readonly sql: SqlClient.SqlClient;
  readonly reactivity: typeof Reactivity.Reactivity.Service;
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | import("effect/unstable/sql/SqlError").SqlError, R>;
  readonly mutation: <A, E, R>(options: {
    readonly keys: ReadonlyArray<string>;
    readonly effect: Effect.Effect<A, E, R>;
    readonly committed: (value: A) => boolean;
  }) => Effect.Effect<A, E | import("effect/unstable/sql/SqlError").SqlError, R>;
  readonly changes: <A, E, R>(options: {
    readonly keys: ReadonlyArray<string>;
    readonly read: Effect.Effect<A, E, R>;
  }) => Stream.Stream<A, E, R>;
  readonly pauseRepairs: Effect.Effect<void>;
  readonly repairNow: Effect.Effect<void>;
  readonly diagnostics: Effect.Effect<CommitBoundaryDiagnostics>;
}

interface ActiveSubscription {
  readonly queue: Queue.Queue<void>;
  readonly cleanup: Effect.Effect<void>;
}

const noop = () => {};

/**
 * Build a driver client with one caller-owned Reactivity instance and expose
 * both services. The official convenience Layers provide Reactivity privately;
 * this make-level graph proves the instance captured by SqlClient is shareable.
 */
export const sharedSqlClientLayer = <A extends SqlClient.SqlClient>(
  makeClient: Effect.Effect<A, never, Scope.Scope | Reactivity.Reactivity>,
): Layer.Layer<SqlClient.SqlClient | Reactivity.Reactivity> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const reactivity = yield* Reactivity.make;
      const client = yield* makeClient.pipe(
        Effect.provideService(Reactivity.Reactivity, reactivity),
      );
      return Context.empty().pipe(
        Context.add(SqlClient.SqlClient, client),
        Context.add(Reactivity.Reactivity, reactivity),
      );
    }),
  );

/**
 * The proof boundary owns outer transactions and subscriber lifetime. Mutation
 * effects join the driver's transactionService and only add commit keys to the
 * owner-local set. The owner invalidates after the driver transaction succeeds.
 */
export const makeCommitBoundary = (
  repairIntervalMs: number,
): Effect.Effect<
  CommitBoundary,
  never,
  Scope.Scope | SqlClient.SqlClient | Reactivity.Reactivity
> =>
  Effect.gen(function* () {
    if (!Number.isFinite(repairIntervalMs) || repairIntervalMs <= 0) {
      return yield* Effect.die(new RangeError("repairIntervalMs must be positive"));
    }
    const sql = yield* SqlClient.SqlClient;
    const reactivity = yield* Reactivity.Reactivity;
    const subscriptions = new Set<ActiveSubscription>();
    let closed = false;
    let repairsPaused = false;
    let activeRegistrations = 0;
    let activeRepairFibers = 0;
    let repairPasses = 0;

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const active = yield* Effect.sync(() => {
          closed = true;
          return [...subscriptions];
        });
        for (const subscription of active) yield* subscription.cleanup;
      }),
    );

    const withTransaction: CommitBoundary["withTransaction"] = (body) =>
      Effect.flatMap(Effect.serviceOption(PendingInvalidations), (owned) => {
        if (Option.isSome(owned)) return body;
        return Effect.flatMap(Effect.serviceOption(sql.transactionService), (ambient) => {
          if (Option.isSome(ambient)) {
            return Effect.die(
              new Error("Commit boundary cannot enter an unowned ambient SQL transaction"),
            );
          }
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

    const mutation: CommitBoundary["mutation"] = ({ keys, effect, committed }) =>
      Effect.flatMap(Effect.serviceOption(sql.transactionService), (ambient) => {
        const run = Effect.tap(effect, (value) =>
          Effect.flatMap(Effect.serviceOption(PendingInvalidations), (pending) => {
            if (Option.isNone(pending)) {
              return Effect.die(
                new Error("Ambient SQL transaction is not owned by the commit boundary"),
              );
            }
            if (committed(value)) for (const key of keys) pending.value.add(key);
            return Effect.void;
          }),
        );
        return Option.isSome(ambient) ? run : withTransaction(run);
      });

    const changes: CommitBoundary["changes"] = ({ keys, read }) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* Effect.acquireRelease(
            Effect.gen(function* () {
              const queue = yield* Queue.make<void>({ capacity: 1, strategy: "dropping" });
              const repairStarted = yield* Deferred.make<void>();
              const repair = Effect.acquireUseRelease(
                Effect.sync(() => {
                  activeRepairFibers += 1;
                }),
                () =>
                  Deferred.succeed(repairStarted, undefined).pipe(
                    Effect.andThen(Effect.sleep(repairIntervalMs)),
                    Effect.andThen(
                      Effect.suspend(() => {
                        if (repairsPaused) return Effect.void;
                        repairPasses += 1;
                        return Queue.offer(queue, undefined);
                      }),
                    ),
                    Effect.asVoid,
                    Effect.repeat(Schedule.forever),
                  ),
                () =>
                  Effect.sync(() => {
                    activeRepairFibers -= 1;
                  }),
              );
              const repairFiber = yield* repair.pipe(Effect.forkScoped);
              yield* Deferred.await(repairStarted);
              let cleaned = false;
              let cancel = noop;
              const activeSubscription: ActiveSubscription = {
                queue,
                cleanup: Effect.suspend(() => {
                  if (cleaned) return Effect.void;
                  cleaned = true;
                  subscriptions.delete(activeSubscription);
                  activeRegistrations -= 1;
                  cancel();
                  return Fiber.interrupt(repairFiber).pipe(Effect.andThen(Queue.shutdown(queue)));
                }),
              };
              const registered = yield* Effect.sync(() => {
                if (closed) return false;
                cancel = reactivity.registerUnsafe(keys, () => {
                  Queue.offerUnsafe(queue, undefined);
                });
                activeRegistrations += 1;
                subscriptions.add(activeSubscription);
                return true;
              });
              if (!registered) {
                yield* Fiber.interrupt(repairFiber);
                yield* Queue.shutdown(queue);
                return yield* Effect.interrupt;
              }
              return activeSubscription;
            }),
            (activeSubscription) => activeSubscription.cleanup,
          );
          // Registration precedes the initial read, so a commit racing that read
          // leaves one retained wake and cannot be missed.
          return Stream.concat(
            Stream.fromEffect(read),
            Stream.fromQueue(subscription.queue).pipe(Stream.mapEffect(() => read)),
          );
        }),
      );

    return {
      sql,
      reactivity,
      withTransaction,
      mutation,
      changes,
      pauseRepairs: Effect.sync(() => {
        repairsPaused = true;
      }),
      repairNow: Effect.gen(function* () {
        const active = yield* Effect.sync(() => {
          repairPasses += 1;
          return [...subscriptions];
        });
        for (const subscription of active) {
          yield* Queue.offer(subscription.queue, undefined);
        }
      }),
      diagnostics: Effect.sync(() => ({
        activeSubscribers: subscriptions.size,
        activeRegistrations,
        activeRepairFibers,
        retainedWakes: [...subscriptions].map(({ queue }) => Queue.sizeUnsafe(queue)),
        repairPasses,
        closed,
      })),
    };
  });

export const commitBoundaryLayer = (
  repairIntervalMs: number,
): Layer.Layer<CommitBoundaryService, never, SqlClient.SqlClient | Reactivity.Reactivity> =>
  Layer.effect(CommitBoundaryService, makeCommitBoundary(repairIntervalMs));

export class CommitBoundaryService extends Context.Service<CommitBoundaryService, CommitBoundary>()(
  "@streamsy/sql-boundary-proof/CommitBoundary",
) {}
