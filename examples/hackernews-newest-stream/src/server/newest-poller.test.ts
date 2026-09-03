/* oxlint-disable effecttsgo/async-function -- This Vitest scenario exercises the demo's Promise compatibility edges through one explicit ManagedRuntime. */
import * as StateProjection from "@streamsy/projection";
import { Effect, ManagedRuntime } from "effect";
import { describe, expect, test } from "vitest";
import type { HnStory } from "../state-schema.ts";
import { type HackerNewsApi } from "./poller/contract.ts";
import { makeNewestStoriesPoller } from "./poller/poller.ts";
import { makeStoryProjection } from "./projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
import { appendSourceBatchFromPromise } from "./streams.ts";
import { demoHarness, story } from "./test-support.ts";

describe("NewestStoriesPoller", () => {
  test("an unchanged second poll appends no source or projection output", async () => {
    const h = await demoHarness();
    const runtime = ManagedRuntime.make(StateProjection.layerClient(h.streams.client));
    const projection = await runtime.runPromise(
      makeStoryProjection({
        pages: 10,
        batches: 10,
        items: 10,
        bytes: 100_000,
      }),
    );
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
            h.streams.appendSourceBatch(changes),
          ),
          catchUpProjection: projection.catchUp,
        },
      }),
    );

    try {
      await runtime.runPromise(poller.pollNow);
      const sourceAfterFirst = await h.adapter.listMessages(hackerNewsSource.streamId);
      const targetAfterFirst = await h.adapter.listMessages(hackerNewsTarget.streamId);

      await runtime.runPromise(poller.pollNow);
      expect(await h.adapter.listMessages(hackerNewsSource.streamId)).toEqual(sourceAfterFirst);
      expect(await h.adapter.listMessages(hackerNewsTarget.streamId)).toEqual(targetAfterFirst);
      expect(await runtime.runPromise(poller.stats)).toMatchObject({
        lastStoryCount: 2,
        lastFetchedNewStories: 0,
        lastRefreshedStories: 2,
        lastChangedStories: 0,
        lastRemovedStories: 0,
        sourceBatches: 1,
        sourceChanges: 2,
      });
      expect(await runtime.runPromise(projection.status)).toMatchObject({
        lastOutcome: { status: "caught-up", progress: { batches: 0, items: 0 } },
      });
    } finally {
      await runtime.runPromise(poller.stop);
      await runtime.dispose();
      await h.streams.close();
    }
  });
});
