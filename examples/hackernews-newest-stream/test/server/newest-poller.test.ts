/* oxlint-disable effecttsgo/async-function -- This Bun scenario exercises the demo's Promise compatibility edges through one explicit ManagedRuntime. */
import { Effect } from "effect";
import { describe, expect, test } from "bun:test";
import type { HnStory } from "../../src/state-schema.ts";
import { type HackerNewsApi } from "../../src/server/poller/contract.ts";
import { makeNewestStoriesPoller } from "../../src/server/poller/poller.ts";
import { hackerNewsSource } from "../../src/server/stream-resources.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";

describe("NewestStoriesPoller", () => {
  test("an unchanged second poll appends no source output", async () => {
    const h = await demoHarness();
    const runtime = h.runtime;
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
          appendSourceBatch: h.streams.appendSourceBatch,
        },
      }),
    );

    try {
      await runtime.runPromise(poller.pollNow);
      const sourceAfterFirst = await h.read(hackerNewsSource.id);

      await runtime.runPromise(poller.pollNow);
      expect(await h.read(hackerNewsSource.id)).toEqual(sourceAfterFirst);
      expect(await runtime.runPromise(poller.stats)).toMatchObject({
        lastStoryCount: 2,
        lastFetchedNewStories: 0,
        lastRefreshedStories: 2,
        lastChangedStories: 0,
        lastRemovedStories: 0,
        sourceBatches: 1,
        sourceChanges: 2,
      });
    } finally {
      await runtime.dispose();
    }
  });
});
