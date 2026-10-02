/* oxlint-disable effecttsgo/async-function -- Bun owns the executable driver harness. */
import { PgClient } from "@effect/sql-pg";
import { describe, expect, test } from "bun:test";
import { Context, Effect, Exit, Layer, Redacted, Scope } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { freshDatabase, sqlDialects } from "../../storage/test/dialects/support.ts";
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

const url = sqlDialects.pg.url;

describe.skipIf(url === undefined)("Postgres driver", () => {
  test("official Postgres driver proves the SQL commit and bounded wake boundary", async () => {
    if (url === undefined) return;
    const database = await Effect.runPromise(Scope.make());
    const fresh = await Effect.runPromise(
      freshDatabase(sqlDialects.pg, url).pipe(Scope.provide(database)),
    );
    const owner = await Effect.runPromise(Scope.make());
    const client = (maxConnections: number) =>
      sharedSqlClientLayer(PgClient.make({ url: Redacted.make(fresh), maxConnections }));
    const layer = commitBoundaryLayer(20).pipe(Layer.provideMerge(client(10)));
    const context = await Effect.runPromise(Layer.build(layer).pipe(Scope.provide(owner)));
    const boundary = Context.get(context, CommitBoundaryService);
    // A second pool stands in for another process that writes the same database.
    const external = Context.get(
      await Effect.runPromise(Layer.build(client(1)).pipe(Scope.provide(owner))),
      SqlClient.SqlClient,
    );
    const externalWrite =
      external`UPDATE protocol_state SET revision = ${5}, expires_at_ms = ${999} WHERE id = ${"stream"}`.pipe(
        Effect.asVoid,
        Effect.orDie,
      );

    try {
      const result = await Effect.runPromise(
        runBoundaryProof(boundary, externalWrite).pipe(Effect.scoped),
      );
      expect(result).toEqual({
        sharedGraphReads: 2,
        ambientJoined: true,
        rejected: "MutationRejected",
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
    } finally {
      await Effect.runPromise(Scope.close(owner, Exit.void));
      await Effect.runPromise(Scope.close(database, Exit.void));
    }
  });
});
