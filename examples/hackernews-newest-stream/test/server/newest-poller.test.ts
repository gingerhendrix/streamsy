/* oxlint-disable effecttsgo/async-function -- Bun tests own the runtime lifetime and inject competing writes at the service boundary. */
import { expect, test } from "bun:test";
import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Context, Deferred, Effect, Fiber, Option } from "effect";
import { diffNewest } from "../../src/server/poller/reconcile.ts";
import { makeNewestStoriesPoller } from "../../src/server/poller/poller.ts";
import type { HackerNewsApi } from "../../src/server/poller/contract.ts";
import { newestRows, StoryRows } from "../../src/server/newest-rows.ts";
import { HackerNewsStories, storiesAddress } from "../../src/server/stories.ts";
import { demoHarness, story } from "../../src/server/test-support.ts";

const a = story(101, 30, "first");
const b = story(102, 20, "second");
const fetched = { newestIds: new Set([101]), stories: [a] };

test("diff adds a new story", () => {
  expect(diffNewest({}, fetched)).toEqual([DurableState.upsert("hn-story", a)]);
});
test("diff replaces a changed story", () => {
  expect(diffNewest({ "101": { ...a, title: "old" } }, fetched)).toEqual([
    DurableState.upsert("hn-story", a),
  ]);
});
test("diff suppresses unchanged stories", () => {
  expect(diffNewest({ "101": a }, fetched)).toEqual([]);
});
test("diff deletes departed stories", () => {
  expect(diffNewest({ "102": b }, { newestIds: new Set(), stories: [] })).toEqual([
    DurableState.remove("hn-story", b),
  ]);
});
test("diff preserves a failed fetch while its id remains newest, and sorts changes", () => {
  expect(diffNewest({ "101": a }, { newestIds: new Set([101]), stories: [] })).toEqual([]);
  expect(diffNewest({}, { newestIds: new Set([101, 102]), stories: [b, a] })).toEqual([
    DurableState.upsert("hn-story", a),
    DurableState.upsert("hn-story", b),
  ]);
});

const api: HackerNewsApi = {
  fetchNewestStoryIds: () => Effect.succeed([101]),
  fetchStoriesById: () => Effect.succeed([a]),
};

test("unchanged polls append nothing; a fresh poller deletes a downtime departure", async () => {
  const h = await demoHarness();
  try {
    const poller = await h.runtime.runPromise(
      makeNewestStoriesPoller({ limit: 2, intervalMs: 60_000, api }),
    );
    await h.runtime.runPromise(poller.pollNow);
    const before = await h.read();
    await h.runtime.runPromise(poller.pollNow);
    expect(await h.read()).toEqual(before);
    expect(await h.runtime.runPromise(poller.stats)).toMatchObject({
      lastStoryCount: 1,
      lastUpserts: 0,
      lastDeletes: 0,
    });
    // Existing checkpoint and fold state survive poller orchestration restart.
    const restarted = await h.runtime.runPromise(
      makeNewestStoriesPoller({
        limit: 2,
        intervalMs: 60_000,
        api: {
          fetchNewestStoryIds: () => Effect.succeed([102]),
          fetchStoriesById: () => Effect.succeed([b]),
        },
      }),
    );
    await h.runtime.runPromise(restarted.pollNow);
    expect(await h.runtime.runPromise(restarted.stats)).toMatchObject({
      lastStoryCount: 1,
      lastUpserts: 1,
      lastDeletes: 1,
    });
    await h.runtime.runPromise(Projection.run(newestRows));
    expect(
      Option.getOrThrow(await h.runtime.runPromise(Projection.loadState(newestRows, StoryRows))),
    ).toEqual({ "102": b });
  } finally {
    await h.close();
  }
});

for (const exhaust of [false, true]) {
  test(
    exhaust
      ? "CAS stops after three retries and records the error"
      : "CAS retry folds the competing write and rebuilds the diff without refetching",
    async () => {
      const h = await demoHarness();
      let appends = 0;
      let idsFetched = 0;
      let storiesFetched = 0;
      const service = Context.get(h.context, HackerNewsStories);
      const expectedOffsets: (string | undefined)[] = [];
      const raced = Context.add(h.context, HackerNewsStories, {
        ...service,
        append: (params, events, options) =>
          Effect.gen(function* () {
            appends++;
            expectedOffsets.push(options?.expectedOffset);
            if (exhaust || appends === 1) {
              yield* service.append(
                {},
                DurableState.changes(storiesAddress, { offset: ZERO_OFFSET }, [
                  DurableState.upsert("hn-story", b),
                ]),
              );
            }
            return yield* service.append(params, events, options);
          }),
      });
      try {
        const poller = await Effect.runPromise(
          makeNewestStoriesPoller({
            limit: 2,
            intervalMs: 60_000,
            api: {
              fetchNewestStoryIds: () =>
                Effect.sync(() => {
                  idsFetched++;
                  return [101];
                }),
              fetchStoriesById: () =>
                Effect.sync(() => {
                  storiesFetched++;
                  return [a];
                }),
            },
          }).pipe(Effect.provide(raced)),
        );
        await h.runtime.runPromise(poller.pollNow);
        const stats = await h.runtime.runPromise(poller.stats);
        expect(idsFetched).toBe(1);
        expect(storiesFetched).toBe(1);
        expect(expectedOffsets[0]).toBe(ZERO_OFFSET);
        expect(new Set(expectedOffsets).size).toBe(appends);
        expect(stats.totalConflictsRetried).toBe(exhaust ? 3 : 1);
        expect(appends).toBe(exhaust ? 4 : 2);
        expect(stats.polling).toBe(false);
        expect(stats.lastPollCompletedAt).toBeDefined();
        if (exhaust) {
          expect(stats.lastPollError).toContain("OffsetMismatch");
          expect(await h.read()).toHaveLength(4);
        } else {
          expect(stats.lastPollError).toBeUndefined();
          expect(stats).toMatchObject({ lastUpserts: 1, lastDeletes: 1, lastStoryCount: 1 });
          const events = await h.read();
          expect(events.map((event) => [event.key, event.headers.operation])).toEqual([
            ["102", "upsert"],
            ["102", "delete"],
            ["101", "upsert"],
          ]);
          expect(events[1]?.headers.offset).toBe(expectedOffsets[1]);
          await h.runtime.runPromise(Projection.run(newestRows));
          expect(
            Option.getOrThrow(
              await h.runtime.runPromise(Projection.loadState(newestRows, StoryRows)),
            ),
          ).toEqual({ "101": a });
        }
      } finally {
        await h.close();
      }
    },
  );
}

test("pollNow coalesces concurrent callers into one fetch", async () => {
  const h = await demoHarness();
  try {
    await h.runtime.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let fetches = 0;
        const poller = yield* makeNewestStoriesPoller({
          limit: 2,
          intervalMs: 60_000,
          api: {
            fetchNewestStoryIds: () =>
              Effect.gen(function* () {
                fetches++;
                yield* Deferred.succeed(started, undefined);
                yield* Deferred.await(release);
                return [101];
              }),
            fetchStoriesById: api.fetchStoriesById,
          },
        });
        const first = yield* Effect.forkChild(poller.pollNow);
        yield* Deferred.await(started);
        const second = yield* Effect.forkChild(poller.pollNow);
        yield* Effect.yieldNow;
        expect((yield* poller.stats).polling).toBe(true);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        expect(fetches).toBe(1);
      }),
    );
  } finally {
    await h.close();
  }
});
