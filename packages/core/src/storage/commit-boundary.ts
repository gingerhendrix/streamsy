import type { SqlError } from "effect/sql";
import { Context, type Effect } from "effect";

export interface CommitBoundaryApi {
  /**
   * Run `body` in the storage owner's transaction. Nested calls on the same owner
   * join without savepoints, so a failure must escape the outer body to roll back.
   * Same-owner stream writes and records commit with application SQL on that client.
   * With two boundaries over one SQL client, only the outer boundary's hooks and
   * `committed` subscribers see the commit.
   *
   * A nested call on another SQL client opens its own transaction, as does a lone
   * `Storage.mutate` on that client. Its commit comes first and survives an outer
   * rollback. Use one client per database for nested work. With two clients on
   * one database, SQLite can wait until it reports a busy lock. Postgres or MySQL
   * can wait for a row lock that the outer transaction holds.
   *
   * Durable Object clients share the driver's transaction, so a second client
   * joins the first. Using a second object's storage on one fiber is unsupported.
   * `MemoryStorage` dies when work enters another `MemoryStorage` owner.
   *
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
