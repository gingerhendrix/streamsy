import { Context, Effect, Layer } from "effect";
import { CommitBoundary } from "@streamsy/core";
import { ownerOf } from "@streamsy/core/internal";
import { ProjectionFault } from "./fault.ts";

/** The storage owner's transaction, used by Commit.transaction. */
export class Transaction extends Context.Service<
  Transaction,
  {
    readonly owner: object;
    readonly withTransaction: <A, E, R>(
      body: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProjectionFault, R>;
  }
>()("@streamsy/projection/Transaction") {
  /** Join the commit boundary on a memory or SQL storage host. */
  static readonly layer: Layer.Layer<Transaction, never, CommitBoundary> = Layer.effect(
    this,
    Effect.map(CommitBoundary, (boundary) => ({
      owner: ownerOf(boundary),
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
    })),
  );
}
