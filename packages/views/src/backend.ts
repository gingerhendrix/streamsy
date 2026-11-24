import type { Effect } from "effect";
import type { SqlError } from "effect/sql";
import type { ViewDataFault } from "./sql/data-fault.ts";
import type { MemoryEnlistFailure, Store } from "./store.ts";

type CircuitQuery<A> = Effect.Effect<A, SqlError.SqlError | MemoryEnlistFailure>;

export interface Backend {
  readonly validateSource?: (source: string) => ViewDataFault | undefined;
  readonly store: (namespace: string) => Store;
  readonly circuit: (namespace: string) => {
    readonly load: CircuitQuery<string | undefined>;
    readonly save: (hash: string) => CircuitQuery<void>;
  };
  readonly clear: (namespace: string) => CircuitQuery<void>;
  /** Roll back writes made by the unit if it fails. */
  readonly atomic: <A, E, R>(
    run: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError.SqlError | MemoryEnlistFailure, R>;
}
