import { Context, Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { CommitBoundary } from "@streamsy/core";
import { ownerOf } from "@streamsy/core/internal";
import { ProjectionFault } from "./fault.ts";

/** The SQL transaction and stream owner used by Commit.transaction. */
export class Transaction extends Context.Service<
  Transaction,
  {
    readonly owner: object;
    readonly sql: SqlClient.SqlClient;
    readonly withTransaction: <A, E, R>(
      body: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProjectionFault, R>;
  }
>()("@streamsy/projection/Transaction") {
  /**
   * The storage owner's SQL transaction, for Commit.transaction. Works over every SQL
   * storage host. A memory host has no SqlClient, so this Layer does not fit it.
   */
  static readonly layer: Layer.Layer<Transaction, never, CommitBoundary | SqlClient.SqlClient> =
    Layer.effect(
      this,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const boundary = yield* CommitBoundary;

        return {
          owner: ownerOf(boundary),
          sql,
          withTransaction: (body) =>
            boundary.withTransaction(body).pipe(
              Effect.catchTag("SqlError", (cause) =>
                Effect.fail(
                  new ProjectionFault({
                    phase: "checkpoint",
                    reason: "storage-failure",
                    message: "The owner transaction failed",
                    cause,
                  }),
                ),
              ),
            ),
        };
      }),
    );
}
