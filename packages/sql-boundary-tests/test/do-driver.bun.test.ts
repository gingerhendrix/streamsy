/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/node-builtin-import -- Bun owns the retained local workerd harness and unique evidence paths. */
import { mkdtempSync, renameSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { afterEach, expect, test } from "bun:test";
import { Config, Effect } from "effect";
import { Miniflare } from "miniflare";

const scratch = Effect.runSync(
  Config.string("STREAMSY_SQL_BOUNDARY_SCRATCH").pipe(Config.withDefault(tmpdir())),
);
const open: Array<{ readonly miniflare: Miniflare; readonly root: string }> = [];

afterEach(async () => {
  for (const { miniflare, root } of open.splice(0)) {
    await miniflare.dispose();
    renameSync(root, join(scratch, `${basename(root)}-${crypto.randomUUID()}`));
  }
});

test("official Durable Object driver proves the SQL commit and bounded wake boundary in workerd", async () => {
  const root = mkdtempSync(".streamsy-sql-boundary-workerd-");
  const bundle = join(root, "bundle");
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "do-worker.test.ts")],
    outdir: bundle,
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });
  expect(built.success).toBe(true);
  const output = built.outputs[0];
  if (output === undefined) throw new Error("Durable Object proof build produced no output");

  const miniflare = new Miniflare({
    scriptPath: output.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    durableObjects: { PROOF: { className: "SqlBoundaryProofObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });
  open.push({ miniflare, root });
  await miniflare.ready;
  const response = await miniflare.dispatchFetch("http://sql-boundary.test/proof");
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
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
    ownerCleanupPendingBeforeClose: true,
    ownerCleanupPendingInterrupted: true,
    ownerCleanupResourcesBeforeClose: [1, 1, 1],
    ownerCleanupResourcesAfterClose: [0, 0, 0],
    ownerCleanupIdempotentSubscriberClose: true,
    lateSubscriberRejected: true,
    nestedTransactionRejected: true,
  });
});
