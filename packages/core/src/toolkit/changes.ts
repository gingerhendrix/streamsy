import { Context } from "effect";
import type { Storage } from "../storage/storage.ts";

/** A separate requirement for each family's local change feed. Distributes over family unions. */
export type Changes<F> = F extends unknown ? ChangeFeed<F> : never;
interface ChangeFeed<F> {
  readonly _Changes: F;
  readonly _tag: "StreamsyChanges";
}
export interface ChangesApi {
  readonly owner: object;
  readonly changes: (typeof Storage.Service)["changes"];
}
export const Changes = <F>(family: Context.Key<F, unknown>): Context.Key<Changes<F>, ChangesApi> =>
  Context.Service<Changes<F>, ChangesApi>(`@streamsy/core/Changes/${family.key}`);
