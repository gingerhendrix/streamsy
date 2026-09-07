/* oxlint-disable effecttsgo/async-function -- fetch is the workerd executable edge. */
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Effect, Exit, Layer, ManagedRuntime } from "effect";
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

interface ProofObjectId {
  readonly toString: () => string;
}

interface Env {
  readonly PROOF: {
    readonly idFromName: (name: string) => ProofObjectId;
    readonly get: (id: ProofObjectId) => {
      readonly fetch: (input: string) => Promise<Response>;
    };
  };
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.PROOF.get(env.PROOF.idFromName("sql-boundary")).fetch(request.url);
  },
};

export class SqlBoundaryProofObject {
  readonly #ctx: DurableObjectState;

  constructor(ctx: DurableObjectState) {
    this.#ctx = ctx;
  }

  async fetch(): Promise<Response> {
    const clientLayer = sharedSqlClientLayer(SqliteClient.make({ storage: this.#ctx.storage }));
    const layer = commitBoundaryLayer(20).pipe(Layer.provideMerge(clientLayer));
    const runtime = ManagedRuntime.make(layer);
    let disposed = false;
    try {
      const boundary = runtime.runSync(CommitBoundaryService);
      const nested = await runtime.runPromise(
        boundary.sql.withTransaction(boundary.sql.withTransaction(Effect.void)).pipe(Effect.exit),
      );
      const result = await runtime.runPromise(
        runBoundaryProof(
          boundary,
          Effect.sync(() => {
            this.#ctx.storage.sql.exec(
              "UPDATE protocol_state SET revision = 5, expires_at_ms = 999 WHERE id = 'stream'",
            );
          }),
        ).pipe(Effect.scoped),
      );
      const pendingCleanup = await runtime.runPromise(prepareOwnerCleanupProof(boundary));
      await runtime.dispose();
      disposed = true;
      const ownerCleanup = await Effect.runPromise(
        finishOwnerCleanupProof(boundary, pendingCleanup),
      );
      return Response.json({
        ...result,
        ...ownerCleanup,
        nestedTransactionRejected: Exit.isFailure(nested),
      });
    } finally {
      if (!disposed) await runtime.dispose();
    }
  }
}
