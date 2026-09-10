/* oxlint-disable effecttsgo/strict-effect-provide -- The proof composes protocol services over the supplied SQL host. */
/* oxlint-disable eslint/no-underscore-dangle -- Test results use the public tagged-outcome `_tag`. */
import { Cause, Deferred, Effect, Exit, Fiber, Option, Queue, Scope, Stream } from "effect";
import {
  Offset,
  Protocol,
  StreamsReader,
  StreamsWriter,
  Storage,
  StreamId,
  ZERO_OFFSET,
  type Mutation,
  type StreamRecord,
} from "@streamsy/core";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { CommitBoundary, type BoundaryTestProbe } from "../src/boundary.ts";

const one = Offset.make("0000000000000001_0000000000000000");

const record = (text: string): StreamRecord => ({
  id: StreamId.make(text),
  config: { contentType: "text/plain", createdAt: 0 },
  lifecycle: { closed: false, softDeleted: false },
  currentOffset: ZERO_OFFSET,
});

const create = (text: string): Mutation => ({
  operations: [{ _tag: "Create", record: record(text), initialMessages: [] }],
});

const touch = (text: string, expiresAtMs: number): Mutation => ({
  operations: [
    {
      _tag: "Append",
      streamId: StreamId.make(text),
      messages: [],
      patch: { lifecycle: { expiresAtMs } },
    },
  ],
});

const exists = (sql: SqlClient.SqlClient, table: string, id: string) =>
  sql
    .unsafe<{ readonly count: number }>(`SELECT COUNT(*) count FROM ${table} WHERE id=?`, [id])
    .pipe(Effect.map((rows) => rows[0]?.count === 1));

const drain = <A, E, R>(pull: Effect.Effect<ReadonlyArray<A>, E, R>) =>
  Effect.flatMap(pull, (values) => {
    const value = values[0];
    return value === undefined ? Effect.die(new Error("empty stream pull")) : Effect.succeed(value);
  });

const turns = Effect.forEach([1, 2, 3, 4, 5], () => Effect.yieldNow, { discard: true });

const retainedWakes = (probe: BoundaryTestProbe) =>
  [...probe.queues].map((queue) => Queue.sizeUnsafe(queue));

/** Complete full-Storage version of the accepted Batch A ownership/lifetime schedules. */
export const runBoundaryScenarios = (probe: BoundaryTestProbe) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const storage = yield* Storage;
    const boundary = yield* CommitBoundary;
    yield* sql.unsafe(
      "CREATE TABLE IF NOT EXISTS application_state(id TEXT PRIMARY KEY,value TEXT)",
    );
    yield* sql.unsafe("DELETE FROM application_state");

    // Representative records only: this is a host proof, not a Derive store schema.
    const fused = yield* Effect.gen(function* () {
      const reader = yield* StreamsReader;
      const writer = yield* StreamsWriter;
      const id = StreamId.make("fused-sink");
      yield* writer.create(id, { contentType: "text/plain" });
      yield* sql.unsafe("INSERT INTO application_state VALUES ('state','0'),('checkpoint','0')");
      const read = Effect.gen(function* () {
        const output = yield* reader.read(id);
        const rows = yield* sql.unsafe<{ readonly id: string; readonly value: string }>(
          "SELECT id,value FROM application_state WHERE id IN ('state','checkpoint') ORDER BY id",
        );
        return {
          output: output.messages.map((message) => new TextDecoder().decode(message.data)),
          records: rows.map((row) => [row.id, row.value]),
        };
      });
      const write = (value: string) =>
        boundary.withTransaction(
          Effect.gen(function* () {
            yield* writer.append(id, {
              contentType: "text/plain",
              data: new TextEncoder().encode(value),
            });

            yield* sql.unsafe("UPDATE application_state SET value=? WHERE id='state'", [value]);
            yield* sql.unsafe("UPDATE application_state SET value=? WHERE id='checkpoint'", [
              value,
            ]);
            return undefined;
          }),
        );
      const beforeWakes = probe.invalidations;
      const insideWakes = yield* boundary.withTransaction(
        Effect.gen(function* () {
          yield* write("1");
          return probe.invalidations - beforeWakes;
        }),
      );
      const committed = yield* read;
      const commitWakes = probe.invalidations - beforeWakes;
      const failure = yield* boundary
        .withTransaction(write("2").pipe(Effect.andThen(Effect.fail("after-sink"))))
        .pipe(Effect.exit);
      return {
        committed,
        restored: yield* read,
        failed: Exit.isFailure(failure),
        insideWakes,
        commitWakes,
        rollbackWakes: probe.invalidations - beforeWakes - commitWakes,
      };
    }).pipe(Effect.provide(Protocol.layer()));

    let rawBodyRan = false;
    const rawBoundary = yield* sql
      .withTransaction(
        boundary.withTransaction(
          Effect.sync(() => {
            rawBodyRan = true;
          }),
        ),
      )
      .pipe(Effect.exit);

    const rawScope = yield* Scope.make();
    const rawPull = yield* Stream.toPull(storage.changes(StreamId.make("raw"))).pipe(
      Scope.provide(rawScope),
    );
    yield* drain(rawPull);
    const rawPending = yield* drain(rawPull).pipe(Effect.forkScoped, Scope.provide(rawScope));
    const rawMutation = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("INSERT INTO application_state VALUES ('raw','before-defect')");
          // If storage SQL is evaluated before the ownership check, preflight now
          // fails with a missing-table SqlError instead of the structural defect.
          yield* sql.unsafe("DROP TABLE streamsy_streams");
          yield* storage.mutate(create("raw"));
        }),
      )
      .pipe(Effect.exit);
    yield* turns;
    const rawAmbientWakes = rawPending.pollUnsafe() === undefined ? 0 : 1;
    const rawRejectedByOwnership =
      Exit.isFailure(rawMutation) &&
      Cause.pretty(rawMutation.cause).includes("Ambient SQL transaction is not owned");
    yield* Scope.close(rawScope, Exit.void);

    yield* storage.mutate(create("base"));
    const rejection = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("INSERT INTO application_state VALUES ('rejected','before-reject')");
          return yield* storage.mutate(create("base"));
        }),
      )
      .pipe(Effect.flip);

    // Standalone rejection is a typed failure, incurs one attempt and publishes no keys.
    const beforeRejectionAttempts = probe.transactionAttempts;
    const beforeRejectionInvalidations = probe.invalidations;
    const standaloneRejection = yield* storage.mutate(create("base")).pipe(Effect.flip);
    const rejectionAttempts = probe.transactionAttempts - beforeRejectionAttempts;
    const rejectionInvalidations = probe.invalidations - beforeRejectionInvalidations;

    // Recovery inside the outer owner deliberately allows unrelated application SQL to commit.
    const recovered = yield* boundary.withTransaction(
      Effect.gen(function* () {
        yield* sql.unsafe("INSERT INTO application_state VALUES ('recovered','commits')");
        return yield* storage
          .mutate(create("base"))
          .pipe(Effect.catchTag("MutationRejected", Effect.succeed));
      }),
    );

    const nestedScope = yield* Scope.make();
    const nestedPull = yield* Stream.toPull(storage.changes(StreamId.make("nested"))).pipe(
      Scope.provide(nestedScope),
    );
    yield* drain(nestedPull);
    const nestedPending = yield* drain(nestedPull).pipe(
      Effect.forkScoped,
      Scope.provide(nestedScope),
    );
    const nestedApplied = yield* Deferred.make<void>();
    const releaseRollback = yield* Deferred.make<void>();
    const nestedTransaction = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* boundary.withTransaction(storage.mutate(create("nested")));
          yield* Deferred.succeed(nestedApplied, undefined);
          yield* Deferred.await(releaseRollback);
          return yield* Effect.fail("rollback" as const);
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(nestedApplied);
    yield* turns;
    const nestedRollbackWakesBeforeRelease = nestedPending.pollUnsafe() === undefined ? 0 : 1;
    yield* Deferred.succeed(releaseRollback, undefined);
    yield* Fiber.await(nestedTransaction);
    yield* turns;
    const nestedRollbackWakesAfterFailure = nestedPending.pollUnsafe() === undefined ? 0 : 1;
    const nestedRollbackRestored = Option.isNone(yield* storage.record(StreamId.make("nested")));
    yield* Scope.close(nestedScope, Exit.void);

    const rollbackScope = yield* Scope.make();
    const rollbackPull = yield* Stream.toPull(
      storage.changes(StreamId.make("outer-rollback")),
    ).pipe(Scope.provide(rollbackScope));
    yield* drain(rollbackPull);
    const rollbackPending = yield* drain(rollbackPull).pipe(
      Effect.forkScoped,
      Scope.provide(rollbackScope),
    );
    const outerRollback = yield* boundary
      .withTransaction(
        storage
          .mutate(create("outer-rollback"))
          .pipe(Effect.andThen(Effect.fail("outer-rollback" as const))),
      )
      .pipe(Effect.exit);
    yield* turns;
    const outerRollbackWakes = rollbackPending.pollUnsafe() === undefined ? 0 : 1;
    const failureAfterMutation = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("INSERT INTO application_state VALUES ('failure','rolls-back')");
          yield* storage.mutate(create("failure-after-mutation"));
          return yield* Effect.fail("failure-after-mutation" as const);
        }),
      )
      .pipe(Effect.exit);
    yield* turns;
    const failureAfterMutationWakes = rollbackPending.pollUnsafe() === undefined ? 0 : 1;
    yield* Scope.close(rollbackScope, Exit.void);

    const delayedScope = yield* Scope.make();
    const delayedPull = yield* Stream.toPull(storage.changes(StreamId.make("delayed"))).pipe(
      Scope.provide(delayedScope),
    );
    yield* drain(delayedPull);
    const delayedPending = yield* drain(delayedPull).pipe(
      Effect.forkScoped,
      Scope.provide(delayedScope),
    );
    const delayedApplied = yield* Deferred.make<void>();
    const releaseCommit = yield* Deferred.make<void>();
    const delayedTransaction = yield* boundary
      .withTransaction(
        Effect.gen(function* () {
          yield* boundary.withTransaction(storage.mutate(create("delayed")));
          yield* storage.mutate({
            operations: [
              {
                _tag: "Append",
                streamId: StreamId.make("delayed"),
                messages: [{ offset: one, timestamp: 1, data: new TextEncoder().encode("a") }],
                patch: { currentOffset: one },
              },
            ],
          });
          yield* Deferred.succeed(delayedApplied, undefined);
          yield* Deferred.await(releaseCommit);
        }),
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.await(delayedApplied);
    yield* turns;
    const wakesBeforeDelayedCommit = delayedPending.pollUnsafe() === undefined ? 0 : 1;
    yield* Deferred.succeed(releaseCommit, undefined);
    yield* Fiber.join(delayedTransaction);
    const delayedSnapshot = yield* Fiber.join(delayedPending).pipe(Effect.timeout("5 seconds"));
    yield* Scope.close(delayedScope, Exit.void);

    const fanoutScope = yield* Scope.make();
    const fanoutId = StreamId.make("fanout");
    const fastPull = yield* Stream.toPull(storage.changes(fanoutId)).pipe(
      Scope.provide(fanoutScope),
    );
    const slowPull = yield* Stream.toPull(storage.changes(fanoutId)).pipe(
      Scope.provide(fanoutScope),
    );
    yield* drain(fastPull);
    yield* drain(slowPull);
    yield* boundary.withTransaction(
      Effect.gen(function* () {
        yield* storage.mutate(create("fanout"));
        yield* storage.mutate(touch("fanout", 100));
      }),
    );
    const coalescedRetainedWakes = retainedWakes(probe);
    const fastSnapshot = yield* drain(fastPull);
    for (let ttl = 101; ttl <= 200; ttl++) yield* storage.mutate(touch("fanout", ttl));
    const burstRetainedWakes = retainedWakes(probe);
    const slowSnapshot = yield* drain(slowPull);
    yield* drain(fastPull);
    const emptyAfterBurst = retainedWakes(probe);
    yield* storage.mutate(touch("fanout", 201));
    const ttlOnlyRetainedWakes = retainedWakes(probe);
    const ttlOnlySnapshot = yield* drain(fastPull);
    yield* drain(slowPull);
    const ttlRecord = Option.getOrThrow(yield* storage.record(fanoutId));
    yield* Scope.close(fanoutScope, Exit.void);
    const subscriberCleanupCounts = [
      probe.queues.size,
      probe.activeRegistrations,
      probe.activeRepairFibers,
    ] as const;

    const raceRegistered = yield* Deferred.make<void>();
    const releaseRaceRead = yield* Deferred.make<void>();
    probe.afterRegister = Deferred.succeed(raceRegistered, undefined).pipe(
      Effect.andThen(Deferred.await(releaseRaceRead)),
    );
    const racing = yield* storage
      .changes(StreamId.make("initial-race"))
      .pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped);
    yield* Deferred.await(raceRegistered);
    probe.afterRegister = undefined;
    yield* storage.mutate(create("initial-race"));
    yield* Deferred.succeed(releaseRaceRead, undefined);
    const initialRaceSnapshots = yield* Fiber.join(racing).pipe(Effect.timeout("5 seconds"));

    return {
      fused,
      rawBoundaryRejectedBeforeBody: Exit.isFailure(rawBoundary) && !rawBodyRan,
      rawMutationDefectedBeforeStorageSql: rawRejectedByOwnership,
      rawApplicationRolledBack: !(yield* exists(sql, "application_state", "raw")),
      rawStorageRolledBack: Option.isNone(yield* storage.record(StreamId.make("raw"))),
      rawAmbientWakes,
      rejection: rejection._tag,
      rejectionApplicationRolledBack: !(yield* exists(sql, "application_state", "rejected")),
      standaloneRejection: standaloneRejection._tag,
      rejectionAttempts,
      rejectionInvalidations,
      recoveredRejection: recovered._tag,
      recoveredApplicationCommitted: yield* exists(sql, "application_state", "recovered"),
      nestedRollbackWakesBeforeRelease,
      nestedRollbackWakesAfterFailure,
      nestedRollbackRestored,
      outerRollbackRestored:
        Exit.isFailure(outerRollback) &&
        Option.isNone(yield* storage.record(StreamId.make("outer-rollback"))),
      outerRollbackWakes,
      failureAfterMutationRolledBack:
        Exit.isFailure(failureAfterMutation) &&
        Option.isNone(yield* storage.record(StreamId.make("failure-after-mutation"))) &&
        !(yield* exists(sql, "application_state", "failure")),
      failureAfterMutationWakes,
      wakesBeforeDelayedCommit,
      wakesAfterDelayedCommit: 1,
      delayedSnapshotPresent: delayedSnapshot.present,
      delayedOffset: delayedSnapshot.currentOffset,
      fastSnapshotPresent: fastSnapshot.present,
      slowSnapshotPresent: slowSnapshot.present,
      coalescedRetainedWakes,
      burstRetainedWakes,
      emptyAfterBurst,
      ttlOnlyRetainedWakes,
      ttlOnlySnapshotPresent: ttlOnlySnapshot.present,
      ttlOnlyExpiresAtMs: ttlRecord.lifecycle.expiresAtMs,
      initialRaceSnapshots: [...initialRaceSnapshots].map(({ present }) => present),
      subscriberCleanupCounts,
    };
  });

export const runExternalRepair = (externalWrite: Effect.Effect<void>, probe: BoundaryTestProbe) =>
  Effect.gen(function* () {
    const storage = yield* Storage;
    const id = StreamId.make("external");
    const pull = yield* Stream.toPull(storage.changes(id));
    yield* drain(pull);
    const pending = yield* drain(pull).pipe(Effect.forkScoped);
    yield* turns;
    const repairPassesBefore = probe.repairPasses;
    yield* externalWrite;
    const parkedAfterCommit = pending.pollUnsafe() === undefined;
    const value = yield* Fiber.join(pending).pipe(Effect.timeout("5 seconds"));
    return {
      parkedAfterCommit,
      repaired: value.present,
      repairPasses: probe.repairPasses - repairPassesBefore,
    };
  });

export interface PendingOwnerCleanupProof {
  readonly subscriberScope: Scope.Closeable;
  readonly pending: Fiber.Fiber<unknown, unknown>;
  readonly storage: ReturnType<typeof Storage.of>;
  readonly pendingBeforeClose: boolean;
  readonly resourcesBeforeClose: readonly [number, number, number];
}

export const prepareOwnerCleanupProof = (probe: BoundaryTestProbe) =>
  Effect.gen(function* () {
    const storage = yield* Storage;
    const subscriberScope = yield* Scope.make();
    const pull = yield* Stream.toPull(storage.changes(StreamId.make("owner-cleanup"))).pipe(
      Scope.provide(subscriberScope),
    );
    yield* drain(pull);
    const pending = yield* drain(pull).pipe(Effect.forkScoped, Scope.provide(subscriberScope));
    yield* turns;
    return {
      subscriberScope,
      pending,
      storage,
      pendingBeforeClose: pending.pollUnsafe() === undefined,
      resourcesBeforeClose: [
        probe.queues.size,
        probe.activeRegistrations,
        probe.activeRepairFibers,
      ] as const,
    } satisfies PendingOwnerCleanupProof;
  });

export const finishOwnerCleanupProof = (
  probe: BoundaryTestProbe,
  pending: PendingOwnerCleanupProof,
) =>
  Effect.gen(function* () {
    const pendingExit = yield* Fiber.await(pending.pending);
    const afterOwnerClose = [
      probe.queues.size,
      probe.activeRegistrations,
      probe.activeRepairFibers,
    ] as const;
    yield* Scope.close(pending.subscriberScope, Exit.void);
    yield* Scope.close(pending.subscriberScope, Exit.void);
    const afterIdempotentSubscriberClose = [
      probe.queues.size,
      probe.activeRegistrations,
      probe.activeRepairFibers,
    ] as const;
    const late = yield* pending.storage
      .changes(StreamId.make("late"))
      .pipe(Stream.runCollect, Effect.scoped, Effect.exit);
    return {
      pendingBeforeClose: pending.pendingBeforeClose,
      pendingInterrupted: Exit.isFailure(pendingExit) && Cause.hasInterrupts(pendingExit.cause),
      resourcesBeforeClose: pending.resourcesBeforeClose,
      afterOwnerClose,
      afterIdempotentSubscriberClose,
      lateSubscriberRejected: Exit.isFailure(late),
      ownerClosed: probe.closed,
    };
  });
