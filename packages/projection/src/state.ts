import { Context, type Effect, type Option } from "effect";
import { recordKey, type EncodedStore, type ProjectionKey } from "./checkpoint.ts";
import type { ProjectionFault } from "./fault.ts";

export interface StateApi {
  readonly load: (key: ProjectionKey) => Effect.Effect<Option.Option<string>, ProjectionFault>;
  /** Overwrites the row; must run inside the owner transaction. */
  readonly save: (key: ProjectionKey, encoded: string) => Effect.Effect<void, ProjectionFault>;
  /** Deletes the row inside the owner transaction. Absent is not an error. */
  readonly remove: (key: ProjectionKey) => Effect.Effect<void, ProjectionFault>;
}
/** One encoded state row per projection key, sharing the checkpoint's owner transaction. */
export class State extends Context.Service<State, StateApi>()("@streamsy/projection/State") {}

export const stateFromStore = (store: EncodedStore): StateApi => ({
  load: (key) => store.readState(recordKey(key)),
  save: (key, encoded) => store.writeState(recordKey(key), encoded),
  remove: (key) => store.removeState(recordKey(key)),
});
