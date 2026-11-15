import type { SqlError } from "effect/sql";
import { StorageFault } from "@streamsy/core";

/** Preserve SQL retryability at the runner's storage boundary. */
export const storageFault = (operation: string) => (cause: SqlError.SqlError) =>
  new StorageFault({ operation, message: cause.message, retryable: cause.isRetryable, cause });
