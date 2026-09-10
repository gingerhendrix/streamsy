import type { Effect } from "effect";
import type { CommitApi } from "./commit.ts";
import type { DeriveFault } from "./fault.ts";

export interface Sink<A> {
  readonly identity: string;
  readonly initialPosition: string;
  readonly owner: CommitApi;
  /** Called on the owner's fiber inside its transaction; no external effects. */
  readonly write: (
    outputs: ReadonlyArray<A>,
    previousPosition: string,
  ) => Effect.Effect<string, DeriveFault>;
}
