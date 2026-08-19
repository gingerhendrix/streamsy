import type { HnStory } from "../state-schema.ts";
import { fetchNewestStoryIds, fetchStoriesById, newestStorySort } from "./hnews.ts";
import {
  sourceDelete,
  sourceUpsert,
  type HackerNewsSourceChange,
} from "./story-index-projection.ts";

export type PollerSink = {
  appendSourceBatch(changes: readonly HackerNewsSourceChange[]): Promise<string>;
  catchUpProjection(): Promise<unknown>;
};

export type HackerNewsApi = {
  fetchNewestStoryIds(limit: number): Promise<number[]>;
  fetchStoriesById(ids: readonly number[]): Promise<HnStory[]>;
};

type PollerConfig = {
  limit: number;
  intervalMs: number;
  sink: PollerSink;
  api?: HackerNewsApi;
};

export type PollStats = {
  polling: boolean;
  stopped: boolean;
  lastPollStartedAt?: string;
  lastPollCompletedAt?: string;
  lastPollError?: string;
  lastStoryCount: number;
  lastFetchedNewStories: number;
  lastRefreshedStories: number;
  lastChangedStories: number;
  lastRemovedStories: number;
  lastSourceOffset?: string;
  sourceBatches: number;
  sourceChanges: number;
};

const liveApi: HackerNewsApi = { fetchNewestStoryIds, fetchStoriesById };

export class NewestStoriesPoller {
  private readonly storiesById = new Map<number, HnStory>();
  private readonly api: HackerNewsApi;
  private timer: Timer | undefined;
  private activePoll: Promise<void> | undefined;
  private stopped = false;
  private lastPollStartedAt: string | undefined;
  private lastPollCompletedAt: string | undefined;
  private lastPollError: string | undefined;
  private lastFetchedNewStories = 0;
  private lastRefreshedStories = 0;
  private lastChangedStories = 0;
  private lastRemovedStories = 0;
  private lastSourceOffset: string | undefined;
  private sourceBatches = 0;
  private sourceChanges = 0;

  constructor(private readonly config: PollerConfig) {
    this.api = config.api ?? liveApi;
  }

  start(): void {
    if (this.timer || this.stopped) return;
    void this.pollNow();
    this.timer = setInterval(() => void this.pollNow(), this.config.intervalMs);
  }

  async close(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.activePoll;
  }

  pollNow(): Promise<void> {
    if (this.stopped) return Promise.resolve();
    if (this.activePoll) return this.activePoll;
    const poll = this.runPoll().finally(() => {
      if (this.activePoll === poll) this.activePoll = undefined;
    });
    this.activePoll = poll;
    return poll;
  }

  stats(): PollStats {
    return {
      polling: this.activePoll !== undefined,
      stopped: this.stopped,
      lastPollStartedAt: this.lastPollStartedAt,
      lastPollCompletedAt: this.lastPollCompletedAt,
      lastPollError: this.lastPollError,
      lastStoryCount: this.storiesById.size,
      lastFetchedNewStories: this.lastFetchedNewStories,
      lastRefreshedStories: this.lastRefreshedStories,
      lastChangedStories: this.lastChangedStories,
      lastRemovedStories: this.lastRemovedStories,
      lastSourceOffset: this.lastSourceOffset,
      sourceBatches: this.sourceBatches,
      sourceChanges: this.sourceChanges,
    };
  }

  private async runPoll(): Promise<void> {
    this.lastPollStartedAt = new Date().toISOString();
    this.lastPollError = undefined;
    this.lastFetchedNewStories = 0;
    this.lastRefreshedStories = 0;
    this.lastChangedStories = 0;
    this.lastRemovedStories = 0;

    try {
      const newestIds = [...new Set(await this.api.fetchNewestStoryIds(this.config.limit))].slice(
        0,
        this.config.limit,
      );
      const newestIdSet = new Set(newestIds);
      const newIds = newestIds.filter((id) => !this.storiesById.has(id));
      const refreshIds = newestIds.filter((id) => this.storiesById.has(id));
      const [newStories, refreshedStories] = await Promise.all([
        this.api.fetchStoriesById(newIds),
        this.api.fetchStoriesById(refreshIds),
      ]);
      this.lastFetchedNewStories = newStories.length;
      this.lastRefreshedStories = refreshedStories.length;

      const nextStories = new Map([...this.storiesById].filter(([id]) => newestIdSet.has(id)));
      for (const story of [...newStories, ...refreshedStories]) {
        if (newestIdSet.has(story.id)) nextStories.set(story.id, story);
      }

      const removed = [...this.storiesById.values()]
        .filter((story) => !nextStories.has(story.id))
        .toSorted((a, b) => a.id - b.id);
      const changed = [...nextStories.values()]
        .filter((story) => {
          const previous = this.storiesById.get(story.id);
          return previous === undefined || !storyEquals(previous, story);
        })
        .toSorted(newestStorySort);
      const sourceChanges: HackerNewsSourceChange[] = [
        ...removed.map(sourceDelete),
        ...changed.map(sourceUpsert),
      ];

      if (sourceChanges.length > 0) {
        this.lastSourceOffset = await this.config.sink.appendSourceBatch(sourceChanges);
        this.sourceBatches += 1;
        this.sourceChanges += sourceChanges.length;
        replaceMap(this.storiesById, nextStories);
      }

      await this.config.sink.catchUpProjection();
      this.lastChangedStories = changed.length;
      this.lastRemovedStories = removed.length;
      this.lastPollCompletedAt = new Date().toISOString();
      console.log(
        `HN poll fetched ${newStories.length} new, refreshed ${refreshedStories.length}, upserted ${changed.length}, removed ${removed.length}`,
      );
    } catch (error) {
      this.lastPollError = error instanceof Error ? error.message : String(error);
      console.error("HN poll failed", error);
    }
  }
}

function replaceMap(target: Map<number, HnStory>, source: ReadonlyMap<number, HnStory>): void {
  target.clear();
  for (const [id, story] of source) target.set(id, story);
}

function storyEquals(a: HnStory, b: HnStory): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
