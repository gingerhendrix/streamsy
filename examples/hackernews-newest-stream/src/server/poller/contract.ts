import { Context, Effect, Schema } from "effect";
import type { HnStory } from "../../state-schema.ts";
import type { ProjectionServices } from "../projection.ts";
import type { HackerNewsSourceChange } from "../source-change.ts";
import { errorMessage } from "../util.ts";

/** Expected operational failure from one poll pass boundary. */
export class PollFailure extends Schema.TaggedError<PollFailure>()(
  "NewestStoriesPoller.PollFailure",
  { operation: Schema.String, reason: Schema.String },
) {}

export const pollFailure = (operation: string) => (cause: unknown) =>
  new PollFailure({ operation, reason: errorMessage(cause) });

export type { ProjectionServices };

export type PollCounters = {
  readonly lastPollStartedAt?: string;
  readonly lastPollCompletedAt?: string;
  readonly lastPollError?: string;
  readonly lastFetchedNewStories: number;
  readonly lastRefreshedStories: number;
  readonly lastChangedStories: number;
  readonly lastRemovedStories: number;
  readonly lastSourceOffset?: string;
  readonly sourceBatches: number;
  readonly sourceChanges: number;
};

export type PollStats = PollCounters & {
  readonly polling: boolean;
  readonly stopped: boolean;
  readonly lastStoryCount: number;
};

export type PollerSink = {
  readonly appendSourceBatch: (
    changes: readonly HackerNewsSourceChange[],
  ) => Effect.Effect<string, PollFailure>;
  readonly catchUpProjection: Effect.Effect<unknown, never, ProjectionServices>;
};

export type HackerNewsApi = {
  readonly fetchNewestStoryIds: (limit: number) => Effect.Effect<number[], PollFailure>;
  readonly fetchStoriesById: (ids: readonly number[]) => Effect.Effect<HnStory[], PollFailure>;
};

export type PollerConfig = {
  readonly limit: number;
  readonly intervalMs: number;
  readonly sink: PollerSink;
  readonly api?: HackerNewsApi;
};

export interface NewestStoriesPollerService {
  /** One coalesced poll pass. Joins the in-flight pass instead of queueing another. */
  readonly pollNow: Effect.Effect<void, never, ProjectionServices>;
  /** Fork the interval polling loop. The first pass runs immediately. */
  readonly start: Effect.Effect<void, never, ProjectionServices>;
  /** Stop the loop and wait for any in-flight pass to complete. */
  readonly stop: Effect.Effect<void>;
  readonly stats: Effect.Effect<PollStats>;
}

export class NewestStoriesPoller extends Context.Service<
  NewestStoriesPoller,
  NewestStoriesPollerService
>()("HackerNews/NewestStoriesPoller") {}

export const initialCounters: PollCounters = {
  lastFetchedNewStories: 0,
  lastRefreshedStories: 0,
  lastChangedStories: 0,
  lastRemovedStories: 0,
  sourceBatches: 0,
  sourceChanges: 0,
};
