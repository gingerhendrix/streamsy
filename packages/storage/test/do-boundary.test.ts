/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/node-builtin-import -- Bun owns the local workerd harness and retained state. */
import { cpSync, mkdtempSync, rmSync } from "node:fs";
import { basename, join } from "node:path";
import { afterEach, expect, test } from "vitest";
import { Config, Effect } from "effect";
import { Miniflare } from "miniflare";

const scratch = Effect.runSync(
  Config.String("STREAMSY_STORAGE_SCRATCH").pipe(Config.withDefault("/tmp")),
);

const open: Array<{ readonly miniflare: Miniflare; readonly root: string }> = [];

afterEach(async () => {
  for (const { miniflare, root } of open.splice(0)) {
    await miniflare.dispose();
    cpSync(root, join(scratch, `${basename(root)}-${crypto.randomUUID()}`), { recursive: true });
    rmSync(root, { recursive: true });
  }
});

test("full Storage boundary schedules pass on the official DO driver in real local workerd", async () => {
  const root = mkdtempSync(".streamsy-storage-workerd-");
  const bundle = join(root, "bundle");

  const built = await Bun.build({
    entrypoints: [join(import.meta.dirname, "support/do-worker.ts")],
    outdir: bundle,
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  expect(built.success).toBe(true);
  const output = built.outputs[0];

  if (output === undefined) throw new Error("DO Storage proof produced no bundle");

  const miniflare = new Miniflare({
    scriptPath: output.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    durableObjects: { STORAGE: { className: "StorageObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });

  open.push({ miniflare, root });
  await miniflare.ready;
  const response = await miniflare.dispatchFetch("http://storage.test/proof");
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
    boundary: {
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
      delayedOffset: "0000000000000001_0000000000000000",
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
    },
    ownerCleanup: {
      pendingBeforeClose: true,
      pendingInterrupted: true,
      resourcesBeforeClose: [1, 1, 1],
      afterOwnerClose: [0, 0, 0],
      afterIdempotentSubscriberClose: [0, 0, 0],
      lateSubscriberRejected: true,
      ownerClosed: true,
    },
    repair: { parkedAfterCommit: true, repaired: true, repairPasses: 1 },
    sharedStorage: {
      distinctBoundaries: true,
      visibleInside: true,
      failedAsExpected: true,
      rolledBack: true,
    },
    entry: {
      outcome: "Applied",
      present: true,
      capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
      marksContractPassed: true,
      markSummaries: {
        empty: { fresh: 0, sleeping: 0, retryDue: 0, backingOff: 0, stopped: 0, nextDueAt: null },
        mixed: { fresh: 1, sleeping: 0, retryDue: 2, backingOff: 1, stopped: 0, nextDueAt: 0 },
        allDue: { fresh: 0, sleeping: 0, retryDue: 3, backingOff: 0, stopped: 0, nextDueAt: 2_000 },
      },
    },
  });
});

test("Durable Object acquisition upgrades v2 marks and can reopen v3", async () => {
  const root = mkdtempSync(".streamsy-storage-upgrade-");

  const built = await Bun.build({
    entrypoints: [join(import.meta.dirname, "support/do-worker.ts")],
    outdir: join(root, "bundle"),
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  expect(built.success).toBe(true);

  const miniflare = new Miniflare({
    scriptPath: built.outputs[0]!.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    durableObjects: { STORAGE: { className: "StorageObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });

  open.push({ miniflare, root });
  await miniflare.ready;

  for (let attempt = 0; attempt < 2; attempt++) {
    const response = await miniflare.dispatchFetch("http://storage.test/upgrade");
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      version: 3,
      row: {
        params: "a",
        seq: 7,
        attempts: 3,
        dueAt: 1234567890123,
        wakeAt: { _id: "Option", _tag: "None" },
        stopped: false,
        failure: { _id: "Option", _tag: "None" },
      },
    });
  }
});
