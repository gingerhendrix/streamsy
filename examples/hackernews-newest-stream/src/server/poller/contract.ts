import { Context, Effect, Schema } from "effect";
import type { HnStory } from "../../state-schema.ts";
import { errorMessage } from "../util.ts";

/** Expected operational failure from one poll pass boundary. */
export class PollFailure extends Schema.TaggedError<PollFailure>()(
  "NewestStoriesPoller.PollFailure",
  { operation: Schema.String, reason: Schema.String },
) {}

export const pollFailure = (operation: string) => (cause: unknown) =>
  new PollFailure({ operation, reason: errorMessage(cause) });

export type PollCounters = {
  readonly lastPollStartedAt?: string;
  readonly lastPollCompletedAt?: string;
  readonly lastPollError?: string;
  readonly lastUpserts: number;
  readonly lastDeletes: number;
  readonly totalConflictsRetried: number;
  readonly lastOffset?: string;
  readonly lastStoryCount: number;
};

export type PollStats = PollCounters & {
  readonly polling: boolean;
  readonly newestLimit: number;
  readonly pollIntervalMs: number;
};

export interface HackerNewsApiService {
  readonly fetchNewestStoryIds: (limit: number) => Effect.Effect<number[], PollFailure>;
  readonly fetchStoriesById: (ids: readonly number[]) => Effect.Effect<HnStory[], PollFailure>;
}

export class HackerNewsApi extends Context.Service<HackerNewsApi, HackerNewsApiService>()(
  "HackerNews/HackerNewsApi",
) {}

export type PollerConfig = {
  readonly limit: number;
  readonly intervalMs: number;
  readonly api: HackerNewsApiService;
};

export interface NewestStoriesPollerService {
  /** One coalesced poll pass. Joins the in-flight pass instead of queueing another. */
  readonly pollNow: Effect.Effect<void>;
  readonly stats: Effect.Effect<PollStats>;
}

export class NewestStoriesPoller extends Context.Service<
  NewestStoriesPoller,
  NewestStoriesPollerService
>()("HackerNews/NewestStoriesPoller") {}

export const initialCounters: PollCounters = {
  lastUpserts: 0,
  lastDeletes: 0,
  totalConflictsRetried: 0,
  lastStoryCount: 0,
};
