/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/node-builtin-import -- Bun owns the executable official-driver harness. */
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect, test } from "bun:test";
import { Config, Effect, Layer, ManagedRuntime } from "effect";
import { Offset } from "@streamsy/core";
import { makeBoundaryTestProbe, sharedSqlClientLayer } from "../src/boundary.ts";
import { layerWithTestProbe } from "../src/storage.ts";
import {
  finishOwnerCleanupProof,
  prepareOwnerCleanupProof,
  runBoundaryScenarios,
  runExternalRepair,
} from "./support/storage-boundary-scenarios.ts";

const scratch = Effect.runSync(
  Config.String("STREAMSY_STORAGE_SCRATCH").pipe(Config.withDefault("/tmp")),
);

const makeLayer = (filename: string, repairIntervalMs: number) => {
  const probe = makeBoundaryTestProbe();
  const client = Layer.effectContext(
    sharedSqlClientLayer(SqliteClient.make({ filename, create: true, busyTimeout: "50 millis" })),
  );
  return {
    layer: layerWithTestProbe({ repairIntervalMs }, probe).pipe(Layer.provideMerge(client)),
    probe,
  };
};

test("full Bun Storage preserves boundary ownership and the bounded missed-wake schedule", async () => {
  const filename = `${scratch}/bun-storage-boundary-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const { layer, probe } = makeLayer(filename, 60_000);
  probe.repairsPaused = true;
  const runtime = ManagedRuntime.make(layer);
  const result = await runtime.runPromise(runBoundaryScenarios(probe).pipe(Effect.scoped));
  const pendingCleanup = await runtime.runPromise(prepareOwnerCleanupProof(probe));
  await runtime.dispose();
  const ownerCleanup = await Effect.runPromise(finishOwnerCleanupProof(probe, pendingCleanup));
  expect(result).toEqual({
    fused: {
      committed: {
        output: ["1"],
        records: [
          ["checkpoint", "1"],
          ["state", "1"],
        ],
      },
      restored: {
        output: ["1"],
        records: [
          ["checkpoint", "1"],
          ["state", "1"],
        ],
      },
      failed: true,
      insideWakes: 0,
      commitWakes: 1,
      rollbackWakes: 0,
    },
    rawBoundaryRejectedBeforeBody: true,
    rawMutationDefectedBeforeStorageSql: true,
    rawApplicationRolledBack: true,
    rawStorageRolledBack: true,
    rawAmbientWakes: 0,
    rejection: "MutationRejected",
    rejectionApplicationRolledBack: true,
    standaloneRejection: "MutationRejected",
    rejectionAttempts: 1,
    rejectionInvalidations: 0,
    recoveredRejection: "MutationRejected",
    recoveredApplicationCommitted: true,
    nestedRollbackWakesBeforeRelease: 0,
    nestedRollbackWakesAfterFailure: 0,
    nestedRollbackRestored: true,
    outerRollbackRestored: true,
    outerRollbackWakes: 0,
    failureAfterMutationRolledBack: true,
    failureAfterMutationWakes: 0,
    wakesBeforeDelayedCommit: 0,
    wakesAfterDelayedCommit: 1,
    delayedSnapshotPresent: true,
    delayedOffset: Offset.make("0000000000000001_0000000000000000"),
    fastSnapshotPresent: true,
    slowSnapshotPresent: true,
    coalescedRetainedWakes: [1, 1],
    burstRetainedWakes: [1, 1],
    emptyAfterBurst: [0, 0],
    ttlOnlyRetainedWakes: [1, 1],
    ttlOnlySnapshotPresent: true,
    ttlOnlyExpiresAtMs: 201,
    initialRaceSnapshots: [true, true],
    subscriberCleanupCounts: [0, 0, 0],
  });
  expect(ownerCleanup).toEqual({
    pendingBeforeClose: true,
    pendingInterrupted: true,
    resourcesBeforeClose: [1, 1, 1],
    afterOwnerClose: [0, 0, 0],
    afterIdempotentSubscriberClose: [0, 0, 0],
    lateSubscriberRejected: true,
    ownerClosed: true,
  });
});

test("a second Bun process is observed only through bounded repair", async () => {
  const filename = `${scratch}/bun-storage-repair-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const externalWrite = Effect.sync(() => {
    const child = Bun.spawnSync([
      process.execPath,
      "-e",
      "import { Database } from 'bun:sqlite'; const db=new Database(Bun.argv.at(-1),{readwrite:true}); db.run(\"INSERT INTO streamsy_streams VALUES ('external','text/plain',NULL,NULL,0,'0000000000000000_0000000000000000',NULL,0,NULL,NULL,NULL,NULL,0,NULL)\"); db.close(false);",
      filename,
    ]);
    if (!child.success) throw new Error(child.stderr.toString());
  });
  const { layer, probe } = makeLayer(filename, 100);
  const runtime = ManagedRuntime.make(layer);
  const result = await runtime.runPromise(
    runExternalRepair(externalWrite, probe).pipe(Effect.scoped),
  );
  await runtime.dispose();
  expect(result.parkedAfterCommit).toBe(true);
  expect(result.repaired).toBe(true);
  expect(result.repairPasses).toBeGreaterThan(0);
});
