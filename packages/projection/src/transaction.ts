import { Context, Effect } from "effect";
import type { SqlClient } from "effect/unstable/sql";
import type { ProjectionFault } from "./fault.ts";

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
>()("@streamsy/projection/Transaction") {}
