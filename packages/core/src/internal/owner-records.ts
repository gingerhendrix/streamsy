import { Context, type Effect, type Option } from "effect";
import type { StorageFault } from "../fault.ts";

/**
 * Encoded records on the storage owner, for the projection checkpoint store.
 * Calls inside `CommitBoundary.withTransaction` join that transaction.
 */
export interface OwnerRecordsApi {
  readonly read: (key: string) => Effect.Effect<Option.Option<string>, StorageFault>;
  /**
   * A snapshot of the owner's encoded entries whose keys start with this exact prefix.
   * A store that cannot list fails with operation `OWNER_RECORDS_LIST_UNSUPPORTED`.
   */
  readonly list: (
    prefix: string,
  ) => Effect.Effect<ReadonlyArray<readonly [key: string, encoded: string]>, StorageFault>;
  readonly write: (key: string, encoded: string) => Effect.Effect<void, StorageFault>;
  readonly remove: (key: string) => Effect.Effect<void, StorageFault>;
}

/** The `StorageFault.operation` of an owner store that cannot list its records. */
export const OWNER_RECORDS_LIST_UNSUPPORTED = "records.list.unsupported";

export class OwnerRecords extends Context.Service<OwnerRecords, OwnerRecordsApi>()(
  "@streamsy/core/internal/OwnerRecords",
) {}
