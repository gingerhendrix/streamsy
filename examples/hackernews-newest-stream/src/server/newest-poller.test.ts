/* oxlint-disable effecttsgo/async-function -- This Bun scenario exercises the demo's Promise compatibility edges through one explicit ManagedRuntime. */
import { ZERO_OFFSET } from "@streamsy/core";
import { Effect } from "effect";
import { describe, expect, test } from "bun:test";
import type { HnStory } from "../state-schema.ts";
import { type HackerNewsApi } from "./poller/contract.ts";
import { makeNewestStoriesPoller } from "./poller/poller.ts";
import { makeStoryProjection } from "./projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
import { demoHarness, story } from "./test-support.ts";

describe("NewestStoriesPoller", () => {
  test("an unchanged second poll appends no source or projection output", async () => {
    const h = await demoHarness();
    const runtime = h.runtime;
    const projection = await runtime.runPromise(makeStoryProjection({ limit: 10 }));
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
          catchUpProjection: projection.catchUp,
        },
      }),
    );

    try {
      await runtime.runPromise(poller.pollNow);
      const sourceAfterFirst = await h.read(hackerNewsSource.id);
      const targetAfterFirst = await h.read(hackerNewsTarget.id);
      const firstOutcome = (await runtime.runPromise(projection.status)).lastOutcome;
      expect(firstOutcome).toMatchObject({ status: "caught-up" });
      expect(firstOutcome?.progress.sourceThrough).not.toBe(ZERO_OFFSET);

      await runtime.runPromise(poller.pollNow);
      expect(await h.read(hackerNewsSource.id)).toEqual(sourceAfterFirst);
      expect(await h.read(hackerNewsTarget.id)).toEqual(targetAfterFirst);
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
        lastOutcome: { status: "caught-up", progress: firstOutcome?.progress },
      });
    } finally {
      await runtime.runPromise(poller.stop);
      await runtime.dispose();
    }
  });
});
