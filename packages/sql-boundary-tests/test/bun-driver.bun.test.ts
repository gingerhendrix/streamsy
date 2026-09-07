/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/node-builtin-import -- Bun owns the executable driver harness and unique retained database path. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { tmpdir } from "node:os";
import { expect, test } from "bun:test";
import { Config, Context, Effect, Exit, Layer, Scope } from "effect";
import {
  CommitBoundaryService,
  commitBoundaryLayer,
  sharedSqlClientLayer,
} from "../src/commit-boundary.ts";
import {
  finishOwnerCleanupProof,
  prepareOwnerCleanupProof,
  runBoundaryProof,
} from "../src/proof-scenarios.ts";

const scratch = Effect.runSync(
  Config.string("STREAMSY_SQL_BOUNDARY_SCRATCH").pipe(Config.withDefault(tmpdir())),
);

test("official Bun driver proves the SQL commit and bounded wake boundary", async () => {
  const filename = `${scratch}/bun-boundary-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const owner = await Effect.runPromise(Scope.make());
  const clientLayer = sharedSqlClientLayer(
    SqliteClient.make({ filename, create: true, busyTimeout: "100 millis" }),
  );
  const layer = commitBoundaryLayer(20).pipe(Layer.provideMerge(clientLayer));
  const context = await Effect.runPromise(Layer.build(layer).pipe(Scope.provide(owner)));
  const boundary = Context.get(context, CommitBoundaryService);
  const externalWrite = Effect.sync(() => {
    const child = Bun.spawnSync([
      process.execPath,
      "-e",
      "import { Database } from 'bun:sqlite'; const db = new Database(Bun.argv.at(-1), { readwrite: true }); db.run(\"UPDATE protocol_state SET revision = 5, expires_at_ms = 999 WHERE id = 'stream'\"); db.close(false);",
      filename,
    ]);
    if (!child.success) {
      throw new Error(`external writer failed: ${child.stderr.toString()}`);
    }
  });

  const result = await Effect.runPromise(
    runBoundaryProof(boundary, externalWrite).pipe(Effect.scoped),
  );
  expect(result).toEqual({
    sharedGraphReads: 2,
    ambientJoined: true,
    rejected: "Rejected",
    rejectionRolledBackApplicationRow: true,
    rawAmbientBoundaryRejectedBeforeBody: true,
    rawAmbientMutationDefected: true,
    rawAmbientRolledBackApplicationRow: true,
    rawAmbientRestoredMutation: true,
    rawAmbientWakes: 0,
    nestedRollbackWakesBeforeRelease: 0,
    nestedRollbackWakesAfterFailure: 0,
    nestedRollbackRestoredMutation: true,
    outerRollbackRestoredMutation: true,
    failureAfterMutationRolledBack: true,
    wakesBeforeDelayedCommit: 0,
    wakesAfterDelayedCommit: 1,
    committedSnapshot: { revision: 2, expiresAtMs: 99 },
    fastSnapshot: { revision: 4, expiresAtMs: 200 },
    slowSnapshot: { revision: 4, expiresAtMs: 300 },
    ttlOnlySnapshot: { revision: 4, expiresAtMs: 300 },
    coalescedRetainedWakes: [1, 1],
    burstRetainedWakes: [1, 1],
    externalPullParkedBeforeCommit: true,
    externalPullParkedAfterCommit: true,
    externalLocalWakes: 0,
    externalRepairPasses: 1,
    externalRepairSnapshot: { revision: 5, expiresAtMs: 999 },
    initialRaceSnapshots: [
      { revision: 6, expiresAtMs: 1_000 },
      { revision: 6, expiresAtMs: 1_000 },
    ],
    subscribersAfterScopeClose: 0,
  });

  const pendingCleanup = await Effect.runPromise(prepareOwnerCleanupProof(boundary));
  await Effect.runPromise(Scope.close(owner, Exit.void));
  expect(await Effect.runPromise(finishOwnerCleanupProof(boundary, pendingCleanup))).toEqual({
    ownerCleanupPendingBeforeClose: true,
    ownerCleanupPendingInterrupted: true,
    ownerCleanupResourcesBeforeClose: [1, 1, 1],
    ownerCleanupResourcesAfterClose: [0, 0, 0],
    ownerCleanupIdempotentSubscriberClose: true,
    lateSubscriberRejected: true,
  });
});
