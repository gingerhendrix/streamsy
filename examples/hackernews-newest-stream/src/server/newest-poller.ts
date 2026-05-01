import type { HnStory } from "../state-schema.ts";
import { fetchNewestStoryIds, fetchStoriesById } from "./hnews.ts";
import type { ServerCollectionWriter } from "./server-collection.ts";

type PollerConfig = {
  limit: number;
  intervalMs: number;
  writer: ServerCollectionWriter<HnStory, number>;
};

type PollStats = {
  polling: boolean;
  stopped: boolean;
  lastPollStartedAt?: string;
  lastPollCompletedAt?: string;
  lastPollError?: string;
  lastStoryCount: number;
  lastFetchedNewStories: number;
  lastRefreshedStories: number;
  lastChangedStories: number;
};

export class NewestStoriesPoller {
  private readonly storiesById = new Map<number, HnStory>();
  private timer: Timer | undefined;
  private polling = false;
  private stopped = false;
  private lastPollStartedAt: string | undefined;
  private lastPollCompletedAt: string | undefined;
  private lastPollError: string | undefined;
  private lastFetchedNewStories = 0;
  private lastRefreshedStories = 0;
  private lastChangedStories = 0;

  constructor(private readonly config: PollerConfig) {}

  start(): void {
    void this.pollNow();
    this.timer = setInterval(() => void this.pollNow(), this.config.intervalMs);
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  async pollNow(): Promise<void> {
    if (this.polling || this.stopped) return;
    this.polling = true;
    this.lastPollStartedAt = new Date().toISOString();
    this.lastPollError = undefined;
    this.lastFetchedNewStories = 0;
    this.lastRefreshedStories = 0;
    this.lastChangedStories = 0;

    try {
      const newestIds = await fetchNewestStoryIds(this.config.limit);
      const unfetchedIds = newestIds.filter((id) => !this.storiesById.has(id));
      const unfetchedIdSet = new Set(unfetchedIds);

      // Phase 1: newest polling only fetches rows we have not seen before.
      const newStories = await fetchStoriesById(unfetchedIds);
      const changedStories = this.collectChangedStories(newStories);
      this.lastFetchedNewStories = newStories.length;

      // Phase 2: refresh every item already in the server collection. This keeps
      // mutable fields such as score and descendants current without re-fetching
      // existing newest rows as part of the newest-list scan above.
      const refreshIds = [...this.storiesById.keys()].filter((id) => !unfetchedIdSet.has(id));
      const refreshedStories = await fetchStoriesById(refreshIds);
      changedStories.push(...this.collectChangedStories(refreshedStories));
      this.lastRefreshedStories = refreshedStories.length;

      this.config.writer.upsertMany(changedStories);
      this.lastChangedStories = changedStories.length;
      this.lastPollCompletedAt = new Date().toISOString();
      console.log(
        `HN poll added ${newStories.length} new stories, refreshed ${refreshedStories.length}, changed ${changedStories.length}`,
      );
    } catch (error) {
      this.lastPollError = error instanceof Error ? error.message : String(error);
      console.error("HN poll failed", error);
    } finally {
      this.polling = false;
    }
  }

  stats(): PollStats {
    return {
      polling: this.polling,
      stopped: this.stopped,
      lastPollStartedAt: this.lastPollStartedAt,
      lastPollCompletedAt: this.lastPollCompletedAt,
      lastPollError: this.lastPollError,
      lastStoryCount: this.storiesById.size,
      lastFetchedNewStories: this.lastFetchedNewStories,
      lastRefreshedStories: this.lastRefreshedStories,
      lastChangedStories: this.lastChangedStories,
    };
  }

  private collectChangedStories(stories: readonly HnStory[]): HnStory[] {
    const changed: HnStory[] = [];

    for (const story of stories) {
      const previous = this.storiesById.get(story.id);
      if (previous && storyEquals(previous, story)) continue;
      this.storiesById.set(story.id, story);
      changed.push(story);
    }

    return changed;
  }
}

function storyEquals(a: HnStory, b: HnStory): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
