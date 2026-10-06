import { Context } from "effect";
import type { Storage } from "../storage/storage.ts";

/** A separate requirement for each stream's local change feed. Distributes over stream unions. */
export type ChangeFeed<F> = F extends unknown ? FeedMarker<F> : never;

interface FeedMarker<F> {
  readonly _ChangeFeed: F;
  readonly _tag: "StreamsyChangeFeed";
}

export interface ChangeFeedApi {
  readonly owner: object;
  readonly changes: (typeof Storage.Service)["changes"];
}

export const ChangeFeed = <F>(
  stream: Context.Key<F, unknown>,
): Context.Key<ChangeFeed<F>, ChangeFeedApi> =>
  Context.Service<ChangeFeed<F>, ChangeFeedApi>(`@streamsy/core/ChangeFeed/${stream.key}`);
