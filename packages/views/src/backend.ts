import type { Effect } from "effect";
import type { SqlError } from "effect/sql";
import type { Query, Store } from "./store.ts";

export interface Backend {
  readonly store: (namespace: string) => Store;
  readonly circuit: (namespace: string) => {
    readonly load: Query<string | undefined>;
    readonly save: (hash: string) => Query<void>;
  };
  readonly clear: (namespace: string) => Query<void>;
  /** Roll back writes made by the unit if it fails. */
  readonly atomic: <A, E, R>(
    run: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError.SqlError, R>;
}
