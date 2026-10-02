import { Context, type Effect, type Option } from "effect";
import type { StorageFault } from "../fault.ts";

/**
 * Encoded records on the storage owner, for the projection checkpoint store.
 * Calls inside `CommitBoundary.withTransaction` join that transaction.
 */
export interface OwnerRecordsApi {
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, StorageFault>;
  readonly write: (key: string, encoded: string) => Effect.Effect<void, StorageFault>;
  readonly remove: (key: string) => Effect.Effect<void, StorageFault>;
}
export class OwnerRecords extends Context.Service<OwnerRecords, OwnerRecordsApi>()(
  "@streamsy/core/internal/OwnerRecords",
) {}
