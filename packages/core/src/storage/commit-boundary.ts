import type { SqlError } from "effect/sql";
import { Context, type Effect } from "effect";

export interface CommitBoundaryApi {
  /**
   * Run `body` in the storage owner's transaction. Nested calls on the same owner
   * join without savepoints, so a failure must escape the outer body to roll back.
   * On SQL hosts, nesting across different SQL clients dies before the inner
   * body runs. Boundaries built over one shared `SqlClient` can join.
   * Same-owner stream writes and records commit together with application SQL on its
   * client. A lone `Storage.mutate` on another SQL client without an ambient
   * transaction opens and commits its own transaction.
   * Keep the body on the calling fiber, and do not wait in it for wakes that only
   * its own commit publishes. Only SQL hosts fail with `SqlError`.
   */
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError.SqlError, R>;
}

/** The transaction of the storage owner. Every Streamsy storage host provides it. */
export class CommitBoundary extends Context.Service<CommitBoundary, CommitBoundaryApi>()(
  "@streamsy/core/CommitBoundary",
) {}
