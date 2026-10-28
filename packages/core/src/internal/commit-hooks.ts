import { Context, Effect, type Scope } from "effect";
import type { SqlError } from "effect/sql";
import type { StreamId } from "../schema/index.ts";

/**
 * Work that joins an owned transaction just before it commits. It receives the
 * stream ids that the transaction created, appended to or deleted.
 */
export type CommitHook = (ids: ReadonlySet<StreamId>) => Effect.Effect<void, SqlError.SqlError>;

/**
 * Commit hooks on the storage owner, for the durable projection marks.
 *
 * A hook runs inside each later owned transaction that wrote at least one stream
 * id, after the body and before COMMIT. Statements in the hook join the
 * transaction, and a hook failure rolls the transaction back. A SQL host treats
 * a hook failure like a body failure, so a retryable `SqlError` replays the
 * whole transaction, hook included. The memory host turns a hook failure into a
 * defect. A hook must not write streams: its ids would not reach other hooks.
 */
export interface CommitHooksApi {
  /** Register `hook` until the scope closes. */
  readonly beforeCommit: (hook: CommitHook) => Effect.Effect<void, never, Scope.Scope>;
}

export class CommitHooks extends Context.Service<CommitHooks, CommitHooksApi>()(
  "@streamsy/core/internal/CommitHooks",
) {}

/** Shared registry for storage boundaries: register in a scope, run in order. */
export const commitHookRegistry = () => {
  const hooks = new Set<{ readonly hook: CommitHook }>();

  const beforeCommit: CommitHooksApi["beforeCommit"] = (hook) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        const entry = { hook };
        hooks.add(entry);

        return entry;
      }),
      (entry) =>
        Effect.sync(() => {
          hooks.delete(entry);
        }),
    ).pipe(Effect.asVoid);

  const run = (ids: ReadonlySet<StreamId>): Effect.Effect<void, SqlError.SqlError> =>
    ids.size === 0 || hooks.size === 0
      ? Effect.void
      : Effect.forEach([...hooks], (entry) => entry.hook(ids), { discard: true });

  return { beforeCommit, run };
};
