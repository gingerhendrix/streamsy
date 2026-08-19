import { Deferred, Effect, Fiber, Option, Ref, Schedule, Schema } from "effect";
import type { HnStory } from "../state-schema.ts";
import { fetchNewestStoryIds, fetchStoriesById, newestStorySort } from "./hnews.ts";
import {
  sourceDelete,
  sourceUpsert,
  type HackerNewsSourceChange,
} from "./story-index-projection.ts";

/** Expected operational failure from one poll pass boundary. */
export class PollFailure extends Schema.TaggedErrorClass<PollFailure>()(
  "NewestStoriesPoller.PollFailure",
  { operation: Schema.String, reason: Schema.String },
) {}

export type PollerSink<R> = {
  readonly appendSourceBatch: (
    changes: readonly HackerNewsSourceChange[],
  ) => Effect.Effect<string, PollFailure, R>;
  readonly catchUpProjection: Effect.Effect<unknown, never, R>;
};

export type HackerNewsApi = {
  readonly fetchNewestStoryIds: (limit: number) => Effect.Effect<number[], PollFailure>;
  readonly fetchStoriesById: (ids: readonly number[]) => Effect.Effect<HnStory[], PollFailure>;
};

export type PollerConfig<R> = {
  readonly limit: number;
  readonly intervalMs: number;
  readonly sink: PollerSink<R>;
  readonly api?: HackerNewsApi;
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

export interface NewestStoriesPoller<R> {
  /** One coalesced poll pass. Joins the in-flight pass instead of queueing another. */
  readonly pollNow: Effect.Effect<void, never, R>;
  /** Fork the interval polling loop. The first pass runs immediately. */
  readonly start: Effect.Effect<void, never, R>;
  /** Stop the loop and wait for any in-flight pass to complete. */
  readonly stop: Effect.Effect<void>;
  readonly stats: Effect.Effect<PollStats>;
}

const pollFailure = (operation: string) => (error: unknown) =>
  new PollFailure({
    operation,
    reason: error instanceof Error ? error.message : String(error),
  });

/** Adapt a Promise-based source append (such as DemoStreams) to the sink contract. */
export const appendSourceBatchFromPromise =
  (append: (changes: readonly HackerNewsSourceChange[]) => Promise<string>) =>
  (changes: readonly HackerNewsSourceChange[]): Effect.Effect<string, PollFailure> =>
    Effect.tryPromise({
      try: () => append(changes),
      catch: pollFailure("appendSourceBatch"),
    });

/** Named Effect adapters over the Promise-based HN Firebase fetch helpers. */
export const liveHackerNewsApi: HackerNewsApi = {
  fetchNewestStoryIds: Effect.fn("HackerNewsApi.fetchNewestStoryIds")(function* (limit: number) {
    return yield* Effect.tryPromise({
      try: () => fetchNewestStoryIds(limit),
      catch: pollFailure("fetchNewestStoryIds"),
    });
  }),
  fetchStoriesById: Effect.fn("HackerNewsApi.fetchStoriesById")(function* (ids: readonly number[]) {
    return yield* Effect.tryPromise({
      try: () => fetchStoriesById(ids),
      catch: pollFailure("fetchStoriesById"),
    });
  }),
};

type PollCounters = {
  lastPollStartedAt?: string;
  lastPollCompletedAt?: string;
  lastPollError?: string;
  lastFetchedNewStories: number;
  lastRefreshedStories: number;
  lastChangedStories: number;
  lastRemovedStories: number;
  lastSourceOffset?: string;
  sourceBatches: number;
  sourceChanges: number;
};

const initialCounters: PollCounters = {
  lastFetchedNewStories: 0,
  lastRefreshedStories: 0,
  lastChangedStories: 0,
  lastRemovedStories: 0,
  sourceBatches: 0,
  sourceChanges: 0,
};

type Reconciliation = {
  readonly nextStories: ReadonlyMap<number, HnStory>;
  readonly removed: readonly HnStory[];
  readonly changed: readonly HnStory[];
  readonly sourceChanges: readonly HackerNewsSourceChange[];
};

/** Pure deterministic newest-set reconciliation with unchanged-write suppression. */
function reconcileNewest(
  previous: ReadonlyMap<number, HnStory>,
  newestIdSet: ReadonlySet<number>,
  fetched: readonly HnStory[],
): Reconciliation {
  const nextStories = new Map([...previous].filter(([id]) => newestIdSet.has(id)));
  for (const story of fetched) {
    if (newestIdSet.has(story.id)) nextStories.set(story.id, story);
  }
  const removed = [...previous.values()]
    .filter((story) => !nextStories.has(story.id))
    .toSorted((a, b) => a.id - b.id);
  const changed = [...nextStories.values()]
    .filter((story) => {
      const prior = previous.get(story.id);
      return prior === undefined || !storyEquals(prior, story);
    })
    .toSorted(newestStorySort);
  return {
    nextStories,
    removed,
    changed,
    sourceChanges: [...removed.map(sourceDelete), ...changed.map(sourceUpsert)],
  };
}

function storyEquals(a: HnStory, b: HnStory): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

export function makeNewestStoriesPoller<R = never>(
  config: PollerConfig<R>,
): Effect.Effect<NewestStoriesPoller<R>> {
  return Effect.gen(function* () {
    const api = config.api ?? liveHackerNewsApi;
    const storiesRef = yield* Ref.make<ReadonlyMap<number, HnStory>>(new Map());
    const countersRef = yield* Ref.make(initialCounters);
    const activeRef = yield* Ref.make(Option.none<Deferred.Deferred<void>>());
    const stoppedRef = yield* Ref.make(false);
    const loopFiberRef = yield* Ref.make(Option.none<Fiber.Fiber<number>>());

    const runPoll = Effect.gen(function* () {
      yield* Ref.update(countersRef, (counters) => ({
        ...counters,
        lastPollStartedAt: new Date().toISOString(),
        lastPollError: undefined,
        lastFetchedNewStories: 0,
        lastRefreshedStories: 0,
        lastChangedStories: 0,
        lastRemovedStories: 0,
      }));

      const pass = Effect.gen(function* () {
        const newestIds = [...new Set(yield* api.fetchNewestStoryIds(config.limit))].slice(
          0,
          config.limit,
        );
        const newestIdSet = new Set(newestIds);
        const previous = yield* Ref.get(storiesRef);
        const newIds = newestIds.filter((id) => !previous.has(id));
        const refreshIds = newestIds.filter((id) => previous.has(id));
        const [newStories, refreshedStories] = yield* Effect.all(
          [api.fetchStoriesById(newIds), api.fetchStoriesById(refreshIds)],
          { concurrency: "unbounded" },
        );
        yield* Ref.update(countersRef, (counters) => ({
          ...counters,
          lastFetchedNewStories: newStories.length,
          lastRefreshedStories: refreshedStories.length,
        }));

        const outcome = reconcileNewest(previous, newestIdSet, [
          ...newStories,
          ...refreshedStories,
        ]);
        if (outcome.sourceChanges.length > 0) {
          const offset = yield* config.sink.appendSourceBatch(outcome.sourceChanges);
          yield* Ref.set(storiesRef, outcome.nextStories);
          yield* Ref.update(countersRef, (counters) => ({
            ...counters,
            lastSourceOffset: offset,
            sourceBatches: counters.sourceBatches + 1,
            sourceChanges: counters.sourceChanges + outcome.sourceChanges.length,
          }));
        }

        yield* config.sink.catchUpProjection;
        yield* Ref.update(countersRef, (counters) => ({
          ...counters,
          lastChangedStories: outcome.changed.length,
          lastRemovedStories: outcome.removed.length,
          lastPollCompletedAt: new Date().toISOString(),
        }));
        yield* Effect.sync(() =>
          console.log(
            `HN poll fetched ${newStories.length} new, refreshed ${refreshedStories.length}, upserted ${outcome.changed.length}, removed ${outcome.removed.length}`,
          ),
        );
      });

      yield* pass.pipe(
        Effect.catch((failure) =>
          Ref.update(countersRef, (counters) => ({
            ...counters,
            lastPollError: failure.reason,
          })).pipe(Effect.andThen(Effect.sync(() => console.error("HN poll failed", failure)))),
        ),
      );
    });

    const pollNow: Effect.Effect<void, never, R> = Effect.gen(function* () {
      if (yield* Ref.get(stoppedRef)) return;
      const claim = yield* Ref.modify(
        activeRef,
        (
          active,
        ): readonly [
          { owner: boolean; gate: Deferred.Deferred<void> },
          Option.Option<Deferred.Deferred<void>>,
        ] => {
          if (Option.isSome(active)) return [{ owner: false, gate: active.value }, active];
          const gate = Deferred.makeUnsafe<void>();
          return [{ owner: true, gate }, Option.some(gate)];
        },
      );
      if (!claim.owner) {
        return yield* Deferred.await(claim.gate);
      }
      // Uninterruptible so stopping the loop waits for the in-flight pass,
      // matching the previous close() semantics.
      yield* runPoll.pipe(
        Effect.ensuring(
          Ref.set(activeRef, Option.none()).pipe(
            Effect.andThen(Deferred.succeed(claim.gate, undefined)),
          ),
        ),
        Effect.uninterruptible,
      );
    });

    const loop = pollNow.pipe(Effect.repeat(Schedule.spaced(config.intervalMs)));

    const start = Effect.gen(function* () {
      const stopped = yield* Ref.get(stoppedRef);
      const existing = yield* Ref.get(loopFiberRef);
      if (stopped || Option.isSome(existing)) return;
      const fiber = yield* Effect.forkDetach(loop, { startImmediately: true });
      yield* Ref.set(loopFiberRef, Option.some(fiber));
    });

    const stop = Effect.gen(function* () {
      yield* Ref.set(stoppedRef, true);
      const fiber = yield* Ref.get(loopFiberRef);
      yield* Ref.set(loopFiberRef, Option.none());
      if (Option.isSome(fiber)) yield* Fiber.interrupt(fiber.value);
      const active = yield* Ref.get(activeRef);
      if (Option.isSome(active)) yield* Deferred.await(active.value);
    });

    const stats = Effect.gen(function* () {
      const counters = yield* Ref.get(countersRef);
      const stories = yield* Ref.get(storiesRef);
      const active = yield* Ref.get(activeRef);
      const stopped = yield* Ref.get(stoppedRef);
      const result: PollStats = {
        polling: Option.isSome(active),
        stopped,
        lastStoryCount: stories.size,
        ...counters,
      };
      return result;
    });

    return { pollNow, start, stop, stats };
  });
}
