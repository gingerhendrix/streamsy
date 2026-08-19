import { createMemoryStorageAdapter } from "@streamsy/core";
import { StateProjection } from "@streamsy/experimental/effect/state-projection";
import { Effect, ManagedRuntime } from "effect";
import { describe, expect, test } from "vitest";
import type { HnStory } from "../state-schema.ts";
import {
  appendSourceBatchFromPromise,
  makeNewestStoriesPoller,
  type HackerNewsApi,
} from "./newest-poller.ts";
import { createStoryProjection } from "./projection.ts";
import { DemoStreams, hackerNewsSource, hackerNewsTarget } from "./streams.ts";

describe("NewestStoriesPoller", () => {
  test("an unchanged second poll appends no source or projection output", async () => {
    const adapter = createMemoryStorageAdapter();
    const streams = new DemoStreams(adapter);
    await streams.start();
    const runtime = ManagedRuntime.make(StateProjection.layerClient(streams.client));
    const projection = createStoryProjection({
      pages: 10,
      batches: 10,
      items: 10,
      bytes: 100_000,
    });
    const stories = new Map<number, HnStory>([
      [101, story(101, 1_700_000_030, "First")],
      [102, story(102, 1_700_000_020, "Second")],
    ]);
    const api: HackerNewsApi = {
      fetchNewestStoryIds: () => Effect.succeed([101, 102]),
      fetchStoriesById: (ids) =>
        Effect.sync(() =>
          ids.flatMap((id) => {
            const found = stories.get(id);
            return found ? [found] : [];
          }),
        ),
    };
    const poller = await runtime.runPromise(
      makeNewestStoriesPoller({
        limit: 2,
        intervalMs: 60_000,
        api,
        sink: {
          appendSourceBatch: appendSourceBatchFromPromise((changes) =>
            streams.appendSourceBatch(changes),
          ),
          catchUpProjection: projection.catchUp(),
        },
      }),
    );

    try {
      await runtime.runPromise(poller.pollNow);
      const sourceAfterFirst = await adapter.listMessages(hackerNewsSource.streamId);
      const targetAfterFirst = await adapter.listMessages(hackerNewsTarget.streamId);

      await runtime.runPromise(poller.pollNow);
      expect(await adapter.listMessages(hackerNewsSource.streamId)).toEqual(sourceAfterFirst);
      expect(await adapter.listMessages(hackerNewsTarget.streamId)).toEqual(targetAfterFirst);
      expect(await runtime.runPromise(poller.stats)).toMatchObject({
        lastStoryCount: 2,
        lastFetchedNewStories: 0,
        lastRefreshedStories: 2,
        lastChangedStories: 0,
        lastRemovedStories: 0,
        sourceBatches: 1,
        sourceChanges: 2,
      });
      expect(projection.status()).toMatchObject({
        lastOutcome: { status: "caught-up", progress: { batches: 0, items: 0 } },
      });
    } finally {
      await runtime.runPromise(poller.stop);
      await runtime.dispose();
      await streams.close();
    }
  });
});

function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}
