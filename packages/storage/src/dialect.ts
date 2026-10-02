import { Effect } from "effect";
import { StorageFault } from "@streamsy/core";
import type { SqlClient } from "effect/unstable/sql/SqlClient";

/** One value for each SQL dialect that Streamsy storage supports. */
export interface DialectSql<A> {
  readonly sqlite: A;
  readonly pg: A;
  readonly mysql: A;
}

export type StorageDialect = keyof DialectSql<unknown>;

/** The client's dialect. Other dialects fail with a non-retryable fault. */
export const storageDialect = (sql: SqlClient): Effect.Effect<StorageDialect, StorageFault> =>
  sql.onDialectOrElse({
    sqlite: () => Effect.succeed("sqlite" as const),
    pg: () => Effect.succeed("pg" as const),
    mysql: () => Effect.succeed("mysql" as const),
    orElse: () =>
      Effect.fail(
        new StorageFault({
          operation: "storage.dialect",
          message: "Unsupported SQL dialect; Streamsy storage supports sqlite, pg and mysql",
          retryable: false,
        }),
      ),
  });
