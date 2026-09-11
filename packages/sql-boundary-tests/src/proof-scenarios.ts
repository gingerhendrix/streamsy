/* oxlint-disable eslint/no-underscore-dangle -- Proof outcomes follow the accepted Effect-owned `_tag` convention. */
import { Cause, Schema, Deferred, Effect, Exit, Fiber, Option, Scope, Stream } from "effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import type { CommitBoundary } from "./commit-boundary.ts";

interface Snapshot {
  readonly revision: number;
  readonly expiresAtMs: number;
}

type MutationApplied = { readonly _tag: "Applied"; readonly ambient: boolean };

class MutationRejected extends Schema.TaggedError<MutationRejected>()("MutationRejected", {
  revision: Schema.Finite,
}) {}

export interface BoundaryProofResult {
  readonly sharedGraphReads: number;
  readonly ambientJoined: boolean;
  readonly rejected: string;
  readonly rejectionRolledBackApplicationRow: boolean;
  readonly rawAmbientBoundaryRejectedBeforeBody: boolean;
  readonly rawAmbientMutationDefected: boolean;
  readonly rawAmbientRolledBackApplicationRow: boolean;
  readonly rawAmbientRestoredMutation: boolean;
  readonly rawAmbientWakes: number;
  readonly nestedRollbackWakesBeforeRelease: number;
  readonly nestedRollbackWakesAfterFailure: number;
  readonly nestedRollbackRestoredMutation: boolean;
  readonly outerRollbackRestoredMutation: boolean;
  readonly failureAfterMutationRolledBack: boolean;
  readonly wakesBeforeDelayedCommit: number;
  readonly wakesAfterDelayedCommit: number;
  readonly committedSnapshot: Snapshot;
  readonly fastSnapshot: Snapshot;
  readonly slowSnapshot: Snapshot;
  readonly ttlOnlySnapshot: Snapshot;
  readonly coalescedRetainedWakes: ReadonlyArray<number>;
  readonly burstRetainedWakes: ReadonlyArray<number>;
  readonly externalPullParkedBeforeCommit: boolean;
  readonly externalPullParkedAfterCommit: boolean;
  readonly externalLocalWakes: number;
  readonly externalRepairPasses: number;
  readonly externalRepairSnapshot: Snapshot;
  readonly initialRaceSnapshots: ReadonlyArray<Snapshot>;
  readonly subscribersAfterScopeClose: number;
}

export interface PendingOwnerCleanupProof {
  readonly subscriberScope: Scope.Closeable;
  readonly pending: Fiber.Fiber<string, Cause.Done>;
  readonly pendingBeforeClose: boolean;
  readonly resourcesBeforeClose: readonly [number, number, number];
}

export interface OwnerCleanupProofResult {
  readonly ownerCleanupPendingBeforeClose: boolean;
  readonly ownerCleanupPendingInterrupted: boolean;
  readonly ownerCleanupResourcesBeforeClose: readonly [number, number, number];
  readonly ownerCleanupResourcesAfterClose: readonly [number, number, number];
  readonly ownerCleanupIdempotentSubscriberClose: boolean;
  readonly lateSubscriberRejected: boolean;
}

const snapshot = (sql: SqlClient.SqlClient): Effect.Effect<Snapshot, SqlError> =>
  Effect.flatMap(
    sql.unsafe<{ readonly revision: number; readonly expires_at_ms: number }>(
      "SELECT revision, expires_at_ms FROM protocol_state WHERE id = ?",
      ["stream"],
    ),
    (rows) => {
      const row = rows[0];
      return row === undefined
        ? Effect.die(new Error("missing protocol fixture"))
        : Effect.succeed({ revision: row.revision, expiresAtMs: row.expires_at_ms });
    },
  );

const applicationRowExists = (
  sql: SqlClient.SqlClient,
  id = "application",
): Effect.Effect<boolean, SqlError> =>
  Effect.map(
    sql.unsafe<{ readonly count: number }>(
      "SELECT COUNT(*) count FROM application_state WHERE id = ?",
      [id],
    ),
    (rows) => rows[0]?.count === 1,
  );

const mutate = (
  boundary: CommitBoundary,
  expectedRevision: number,
  next: Snapshot,
): Effect.Effect<MutationApplied, MutationRejected | SqlError> =>
  boundary.mutation({
    keys: ["stream:stream"],
    effect: Effect.gen(function* () {
      const ambient = yield* Effect.serviceOption(boundary.sql.transactionService);
      const current = yield* snapshot(boundary.sql);
      if (current.revision !== expectedRevision) {
        return yield* new MutationRejected({ revision: current.revision });
      }
      yield* boundary.sql.unsafe(
        "UPDATE protocol_state SET revision = ?, expires_at_ms = ? WHERE id = ?",
        [next.revision, next.expiresAtMs, "stream"],
      );
      return { _tag: "Applied" as const, ambient: Option.isSome(ambient) };
    }),
  });

const drain = <A, E, R>(pull: Effect.Effect<ReadonlyArray<A>, E, R>): Effect.Effect<A, E, R> =>
  Effect.flatMap(pull, (chunk) => {
    const value = chunk[0];
    return value === undefined ? Effect.die(new Error("empty stream pull")) : Effect.succeed(value);
  });

/** Execute the same boundary schedules against whichever official client backs the boundary. */
export const runBoundaryProof = (boundary: CommitBoundary, externalWrite: Effect.Effect<void>) =>
  Effect.gen(function* () {
    const sql = boundary.sql;
    yield* sql.unsafe(
      "CREATE TABLE IF NOT EXISTS protocol_state (id TEXT PRIMARY KEY, revision INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL)",
    );
    yield* sql.unsafe(
      "CREATE TABLE IF NOT EXISTS application_state (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );
    yield* sql.unsafe("DELETE FROM application_state");
    yield* sql.unsafe("DELETE FROM protocol_state");
    yield* sql.unsafe("INSERT INTO protocol_state (id, revision, expires_at_ms) VALUES (?, ?, ?)", [
      "stream",
      0,
      10,
    ]);

    let sharedGraphReads = 0;
    const sharedGraphReady = yield* Deferred.make<void>();
    const sharedGraph = yield* boundary.sql
      .reactive(
        ["shared-graph"],
        Effect.gen(function* () {
          sharedGraphReads += 1;
          yield* Deferred.succeed(sharedGraphReady, undefined);
          return sharedGraphReads;
        }),
      )
      .pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped);
    yield* Deferred.await(sharedGraphReady);
    yield* boundary.reactivity.invalidate(["shared-graph"]);
    yield* Fiber.join(sharedGraph);

    let commitWakes = 0;
    const cancelCommitObservation = boundary.reactivity.registerUnsafe(["stream:stream"], () => {
      commitWakes += 1;
    });

    let rawAmbientBoundaryBodyRan = false;
    const rawAmbientBoundary = yield* sql
      .withTransaction(
        boundary.withTransaction(
          Effect.sync(() => {
            rawAmbientBoundaryBodyRan = true;
          }),
        ),
      )
      .pipe(Effect.exit);

    let rawAmbientWakes = 0;
    const cancelRawAmbientObservation = boundary.reactivity.registerUnsafe(
      ["stream:stream"],
      () => {
        rawAmbientWakes += 1;
      },
    );
    const rawAmbientMutation = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("INSERT INTO application_state (id, value) VALUES (?, ?)", [
            "raw-ambient",
            "before-defect",
          ]);
          yield* mutate(boundary, 0, { revision: 9, expiresAtMs: 90 });
        }),
      )
      .pipe(Effect.exit);
    const afterRawAmbient = yield* snapshot(sql);
    const rawAmbientApplicationRow = yield* applicationRowExists(sql, "raw-ambient");
    cancelRawAmbientObservation();

    const rejected = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("INSERT INTO application_state (id, value) VALUES (?, ?)", [
            "application",
            "before-rejection",
          ]);
          return yield* mutate(boundary, 99, { revision: 1, expiresAtMs: 20 });
        }),
      )
      .pipe(Effect.flip);
    const rejectionRolledBackApplicationRow = !(yield* applicationRowExists(sql));

    const nestedApplied = yield* Deferred.make<void>();
    const releaseNestedRollback = yield* Deferred.make<void>();
    const nestedRollback = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* boundary.withTransaction(mutate(boundary, 0, { revision: 1, expiresAtMs: 20 }));
          yield* Deferred.succeed(nestedApplied, undefined);
          yield* Deferred.await(releaseNestedRollback);
          return yield* Effect.fail("nested-outer-rollback" as const);
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(nestedApplied);
    for (let turn = 0; turn < 20; turn++) yield* Effect.yieldNow;
    const nestedRollbackWakesBeforeRelease = commitWakes;
    yield* Deferred.succeed(releaseNestedRollback, undefined);
    const nestedRollbackExit = yield* Fiber.await(nestedRollback);
    const nestedRollbackWakesAfterFailure = commitWakes;
    const afterNestedRollback = yield* snapshot(sql);

    const outerRollback = yield* boundary
      .withTransaction(
        mutate(boundary, 0, { revision: 1, expiresAtMs: 20 }).pipe(
          Effect.andThen(Effect.fail("outer-rollback" as const)),
        ),
      )
      .pipe(Effect.exit);
    const afterOuterRollback = yield* snapshot(sql);
    const wakesAfterOuterRollback = commitWakes;

    const failureAfterMutation = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("INSERT INTO application_state (id, value) VALUES (?, ?)", [
            "application",
            "before-failure",
          ]);
          yield* mutate(boundary, 0, { revision: 1, expiresAtMs: 20 });
          return yield* Effect.fail("after-applied" as const);
        }),
      )
      .pipe(Effect.exit);
    const afterFailure = yield* snapshot(sql);
    const applicationAfterFailure = yield* applicationRowExists(sql);
    const wakesAfterFailure = commitWakes;

    const applied = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const committing = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          const first = yield* boundary.withTransaction(
            Effect.gen(function* () {
              const outcome = yield* mutate(boundary, 0, { revision: 1, expiresAtMs: 20 });
              yield* mutate(boundary, 1, { revision: 1, expiresAtMs: 99 });
              yield* mutate(boundary, 1, { revision: 2, expiresAtMs: 99 });
              return outcome;
            }),
          );
          yield* Deferred.succeed(applied, undefined);
          yield* Deferred.await(release);
          return first;
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(applied);
    for (let turn = 0; turn < 20; turn++) yield* Effect.yieldNow;
    const wakesBeforeDelayedCommit = commitWakes;
    yield* Deferred.succeed(release, undefined);
    const committed = yield* Fiber.join(committing);
    const wakesAfterDelayedCommit = commitWakes;
    const committedSnapshot = yield* snapshot(sql);
    cancelCommitObservation();

    const subscriberScope = yield* Scope.make();
    const fastPull = yield* Stream.toPull(
      boundary.changes({ keys: ["stream:stream"], read: snapshot(sql) }),
    ).pipe(Scope.provide(subscriberScope));
    const slowPull = yield* Stream.toPull(
      boundary.changes({ keys: ["stream:stream"], read: snapshot(sql) }),
    ).pipe(Scope.provide(subscriberScope));
    yield* drain(fastPull);
    yield* drain(slowPull);

    yield* boundary.withTransaction(
      Effect.gen(function* () {
        yield* mutate(boundary, 2, { revision: 3, expiresAtMs: 100 });
        yield* mutate(boundary, 3, { revision: 3, expiresAtMs: 200 });
        yield* mutate(boundary, 3, { revision: 4, expiresAtMs: 200 });
      }),
    );
    const coalescedRetainedWakes = (yield* boundary.diagnostics).retainedWakes;
    const fastSnapshot = yield* drain(fastPull);

    for (let ttl = 201; ttl <= 300; ttl++) {
      yield* mutate(boundary, 4, { revision: 4, expiresAtMs: ttl });
    }
    const burstRetainedWakes = (yield* boundary.diagnostics).retainedWakes;
    const slowSnapshot = yield* drain(slowPull);
    const ttlOnlySnapshot = yield* drain(fastPull);

    yield* boundary.pauseRepairs;
    for (let turn = 0; turn < 5; turn++) yield* Effect.yieldNow;
    // Empty both bounded queues, then park the pull before an out-of-graph commit.
    for (let pass = 0; pass < 5; pass++) {
      const retained = (yield* boundary.diagnostics).retainedWakes;
      if (retained[0] === 0 && retained[1] === 0) break;
      if ((retained[0] ?? 0) > 0) yield* drain(fastPull);
      if ((retained[1] ?? 0) > 0) yield* drain(slowPull);
    }
    let externalLocalWakes = 0;
    const cancelExternalObservation = boundary.reactivity.registerUnsafe(["stream:stream"], () => {
      externalLocalWakes += 1;
    });
    const repairPassesBeforeExternal = (yield* boundary.diagnostics).repairPasses;
    const parkedExternalPull = yield* drain(fastPull).pipe(Effect.forkScoped);
    for (let turn = 0; turn < 5; turn++) yield* Effect.yieldNow;
    const externalPullParkedBeforeCommit = parkedExternalPull.pollUnsafe() === undefined;
    yield* externalWrite;
    const externalPullParkedAfterCommit = parkedExternalPull.pollUnsafe() === undefined;
    const repairPassesAfterExternal = (yield* boundary.diagnostics).repairPasses;
    yield* boundary.repairNow;
    const externalRepairSnapshot = yield* Fiber.join(parkedExternalPull).pipe(
      Effect.timeout("2 seconds"),
    );
    const repairPassesAfterRepair = (yield* boundary.diagnostics).repairPasses;
    cancelExternalObservation();

    let raceRead = true;
    const racingRead = Effect.suspend(() => {
      if (!raceRead) return snapshot(sql);
      raceRead = false;
      return Effect.andThen(
        boundary.withTransaction(mutate(boundary, 5, { revision: 6, expiresAtMs: 1_000 })),
        snapshot(sql),
      );
    });
    const initialRaceSnapshots = yield* boundary
      .changes({ keys: ["stream:stream"], read: racingRead })
      .pipe(Stream.take(2), Stream.runCollect);

    yield* Scope.close(subscriberScope, Exit.void);
    const subscribersAfterScopeClose = (yield* boundary.diagnostics).activeSubscribers;

    return {
      sharedGraphReads,
      ambientJoined: committed._tag === "Applied" && committed.ambient,
      rejected: rejected._tag,
      rejectionRolledBackApplicationRow,
      rawAmbientBoundaryRejectedBeforeBody:
        Exit.isFailure(rawAmbientBoundary) && !rawAmbientBoundaryBodyRan,
      rawAmbientMutationDefected:
        Exit.isFailure(rawAmbientMutation) && Cause.hasDies(rawAmbientMutation.cause),
      rawAmbientRolledBackApplicationRow: !rawAmbientApplicationRow,
      rawAmbientRestoredMutation: afterRawAmbient.revision === 0,
      rawAmbientWakes,
      nestedRollbackWakesBeforeRelease,
      nestedRollbackWakesAfterFailure,
      nestedRollbackRestoredMutation:
        Exit.isFailure(nestedRollbackExit) && afterNestedRollback.revision === 0,
      outerRollbackRestoredMutation:
        Exit.isFailure(outerRollback) &&
        afterOuterRollback.revision === 0 &&
        wakesAfterOuterRollback === 0,
      failureAfterMutationRolledBack:
        Exit.isFailure(failureAfterMutation) &&
        afterFailure.revision === 0 &&
        !applicationAfterFailure &&
        wakesAfterFailure === 0,
      wakesBeforeDelayedCommit,
      wakesAfterDelayedCommit,
      committedSnapshot,
      fastSnapshot,
      slowSnapshot,
      ttlOnlySnapshot,
      coalescedRetainedWakes,
      burstRetainedWakes,
      externalPullParkedBeforeCommit,
      externalPullParkedAfterCommit,
      externalLocalWakes,
      externalRepairPasses:
        repairPassesAfterExternal - repairPassesBeforeExternal === 0
          ? repairPassesAfterRepair - repairPassesAfterExternal
          : -1,
      externalRepairSnapshot,
      initialRaceSnapshots: [...initialRaceSnapshots],
      subscribersAfterScopeClose,
    };
  });

export const prepareOwnerCleanupProof = (boundary: CommitBoundary) =>
  Effect.gen(function* () {
    yield* boundary.pauseRepairs;
    const subscriberScope = yield* Scope.make();
    const pull = yield* Stream.toPull(
      boundary.changes({ keys: ["owner-cleanup"], read: Effect.succeed("snapshot") }),
    ).pipe(Scope.provide(subscriberScope));
    yield* drain(pull);
    const pending = yield* drain(pull).pipe(Effect.forkScoped, Scope.provide(subscriberScope));
    for (let turn = 0; turn < 5; turn++) yield* Effect.yieldNow;
    const pendingBeforeClose = pending.pollUnsafe() === undefined;
    const diagnostics = yield* boundary.diagnostics;
    return {
      subscriberScope,
      pending,
      pendingBeforeClose,
      resourcesBeforeClose: [
        diagnostics.activeSubscribers,
        diagnostics.activeRegistrations,
        diagnostics.activeRepairFibers,
      ] as const,
    } satisfies PendingOwnerCleanupProof;
  });

export const finishOwnerCleanupProof = (
  boundary: CommitBoundary,
  pending: PendingOwnerCleanupProof,
) =>
  Effect.gen(function* () {
    const pendingExit = yield* Fiber.await(pending.pending);
    const afterOwnerClose = yield* boundary.diagnostics;
    yield* Scope.close(pending.subscriberScope, Exit.void);
    const afterSubscriberClose = yield* boundary.diagnostics;
    const late = yield* boundary
      .changes({ keys: ["late"], read: Effect.succeed("late") })
      .pipe(Stream.runCollect, Effect.scoped, Effect.exit);
    return {
      ownerCleanupPendingBeforeClose: pending.pendingBeforeClose,
      ownerCleanupPendingInterrupted:
        Exit.isFailure(pendingExit) && Cause.hasInterrupts(pendingExit.cause),
      ownerCleanupResourcesBeforeClose: pending.resourcesBeforeClose,
      ownerCleanupResourcesAfterClose: [
        afterOwnerClose.activeSubscribers,
        afterOwnerClose.activeRegistrations,
        afterOwnerClose.activeRepairFibers,
      ] as const,
      ownerCleanupIdempotentSubscriberClose:
        afterSubscriberClose.activeSubscribers === 0 &&
        afterSubscriberClose.activeRegistrations === 0 &&
        afterSubscriberClose.activeRepairFibers === 0,
      lateSubscriberRejected: Exit.isFailure(late),
    } satisfies OwnerCleanupProofResult;
  });
