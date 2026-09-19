import { Deferred, Effect, Layer, Option, Ref, Schedule } from "effect";
import type { HnStory } from "../../state-schema.ts";
import { liveHackerNewsApi } from "../hnews.ts";
import { nowIso } from "../util.ts";
import {
  initialCounters,
  NewestStoriesPoller,
  type NewestStoriesPollerService,
  type PollCounters,
  type PollerConfig,
  type PollStats,
} from "./contract.ts";
import { reconcileNewest } from "./reconcile.ts";

type PollClaim = {
  readonly owner: boolean;
  readonly gate: Deferred.Deferred<void>;
};

export function makeNewestStoriesPoller(
  config: PollerConfig,
): Effect.Effect<NewestStoriesPollerService> {
  return Effect.gen(function* () {
    const api = config.api ?? liveHackerNewsApi;
    const storiesRef = yield* Ref.make<ReadonlyMap<number, HnStory>>(new Map());
    const countersRef = yield* Ref.make(initialCounters);
    const activeRef = yield* Ref.make(Option.none<Deferred.Deferred<void>>());

    const patchCounters = (patch: Partial<PollCounters>) =>
      Ref.update(countersRef, (counters) => ({ ...counters, ...patch }));

    const runPoll = Effect.fn("NewestStoriesPoller.runPoll")(function* () {
      yield* patchCounters({
        lastPollStartedAt: yield* nowIso,
        lastPollError: undefined,
        lastFetchedNewStories: 0,
        lastRefreshedStories: 0,
        lastChangedStories: 0,
        lastRemovedStories: 0,
      });

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
        yield* patchCounters({
          lastFetchedNewStories: newStories.length,
          lastRefreshedStories: refreshedStories.length,
        });

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

        yield* patchCounters({
          lastChangedStories: outcome.changed.length,
          lastRemovedStories: outcome.removed.length,
          lastPollCompletedAt: yield* nowIso,
        });
        yield* Effect.log(
          `HN poll fetched ${newStories.length} new, refreshed ${refreshedStories.length}, upserted ${outcome.changed.length}, removed ${outcome.removed.length}`,
        );
      });

      yield* pass.pipe(
        Effect.catch((failure) =>
          patchCounters({ lastPollError: failure.reason }).pipe(
            Effect.andThen(Effect.logError("HN poll failed", failure)),
          ),
        ),
      );
    });

    const pollNow: Effect.Effect<void> = Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const claim = yield* Ref.modify(
        activeRef,
        (active): readonly [PollClaim, Option.Option<Deferred.Deferred<void>>] => {
          if (Option.isSome(active)) return [{ owner: false, gate: active.value }, active] as const;
          return [{ owner: true, gate }, Option.some(gate)] as const;
        },
      );
      if (!claim.owner) {
        yield* Deferred.await(claim.gate);
        return;
      }
      // Uninterruptible so stopping the loop waits for the in-flight pass,
      // matching the previous close() semantics.
      yield* runPoll().pipe(
        Effect.ensuring(
          Ref.set(activeRef, Option.none()).pipe(
            Effect.andThen(Deferred.succeed(claim.gate, undefined)),
          ),
        ),
        Effect.uninterruptible,
      );
    }).pipe(Effect.withSpan("NewestStoriesPoller.pollNow"));

    const stats = Effect.gen(function* () {
      const [counters, stories, active] = yield* Effect.all([
        Ref.get(countersRef),
        Ref.get(storiesRef),
        Ref.get(activeRef),
      ]);
      const result: PollStats = {
        polling: Option.isSome(active),
        stopped: false,
        lastStoryCount: stories.size,
        ...counters,
      };
      return result;
    });

    return NewestStoriesPoller.of({ pollNow, stats });
  });
}

export type NewestStoriesPollerLayerConfig = PollerConfig;

export const newestStoriesPollerLayer = (config: NewestStoriesPollerLayerConfig) =>
  Layer.effect(
    NewestStoriesPoller,
    Effect.gen(function* () {
      const poller = yield* makeNewestStoriesPoller(config);
      yield* poller.pollNow.pipe(
        Effect.repeat(Schedule.spaced(config.intervalMs)),
        Effect.forkScoped,
      );
      return poller;
    }),
  );
