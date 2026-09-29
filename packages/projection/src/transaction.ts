import { Context, Effect, Option } from "effect";
import type { SqlClient } from "effect/unstable/sql";
import type { ProjectionFault } from "./fault.ts";

/** The transaction and stream owner used by transaction Commits. */
export class Transaction extends Context.Service<
  Transaction,
  {
    readonly owner: object;
    readonly sql: Option.Option<SqlClient.SqlClient>;
    readonly withTransaction: <A, E, R>(
      body: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | ProjectionFault, R>;
  }
>()("@streamsy/projection/Transaction") {}
