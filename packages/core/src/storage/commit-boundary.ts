import type { SqlError } from "effect/sql";
import { Context, type Effect } from "effect";

export interface CommitBoundaryApi {
  /**
   * Run `body` in the storage owner's transaction. A nested call joins the outer
   * transaction (no savepoints), so a failure must escape the outer body to roll back.
   * Stream writes, owner records and, on SQL hosts, application SQL in `body` commit
   * together. Keep the body on the calling fiber, and do not wait in it for wakes that
   * only its own commit publishes. Only SQL hosts fail with `SqlError`.
   */
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError.SqlError, R>;
}

/** The transaction of the storage owner. Every Streamsy storage host provides it. */
export class CommitBoundary extends Context.Service<CommitBoundary, CommitBoundaryApi>()(
  "@streamsy/core/CommitBoundary",
) {}
