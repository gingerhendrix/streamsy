import { Context, type Effect } from "effect";
import type { DeriveFault } from "./fault.ts";
import type { CheckpointStore, StateStore } from "./stores.ts";

export interface CommitApi {
  readonly checkpoints: CheckpointStore;
  readonly states: StateStore;
  readonly withTransaction: <A, E, R>(
    body: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | DeriveFault, R>;
}
/** One host owner supplies both stores and the transaction used by the sink. */
export class Commit extends Context.Service<Commit, CommitApi>()("@streamsy/derive/Commit") {}
