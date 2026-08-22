import { Deferred, Effect, Fiber, Option, Ref, Schedule } from "effect";
import type { HnStory } from "../../state-schema.ts";
import { liveHackerNewsApi } from "../hnews.ts";
import { nowIso } from "../util.ts";
import {
  initialCounters,
  type NewestStoriesPoller,
  type PollCounters,
  type PollerConfig,
  type PollStats,
} from "./contract.ts";
import { reconcileNewest } from "./reconcile.ts";

type PollClaim = {
  readonly owner: boolean;
  readonly gate: Deferred.Deferred<void>;
};

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

        yield* config.sink.catchUpProjection;
        yield* patchCounters({
          lastChangedStories: outcome.changed.length,
          lastRemovedStories: outcome.removed.length,
          lastPollCompletedAt: yield* nowIso,
        });
        yield* Effect.sync(() =>
          console.log(
            `HN poll fetched ${newStories.length} new, refreshed ${refreshedStories.length}, upserted ${outcome.changed.length}, removed ${outcome.removed.length}`,
          ),
        );
      });

      yield* pass.pipe(
        Effect.catch((failure) =>
          patchCounters({ lastPollError: failure.reason }).pipe(
            Effect.andThen(Effect.sync(() => console.error("HN poll failed", failure))),
          ),
        ),
      );
    });

    const pollNow: Effect.Effect<void, never, R> = Effect.gen(function* () {
      if (yield* Ref.get(stoppedRef)) return;
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

    const loop = pollNow.pipe(Effect.repeat(Schedule.spaced(config.intervalMs)));

    const start: Effect.Effect<void, never, R> = Effect.gen(function* () {
      const stopped = yield* Ref.get(stoppedRef);
      const existing = yield* Ref.get(loopFiberRef);
      if (stopped || Option.isSome(existing)) return;
      const fiber = yield* Effect.forkDetach(loop, { startImmediately: true });
      yield* Ref.set(loopFiberRef, Option.some(fiber));
    }).pipe(Effect.withSpan("NewestStoriesPoller.start"));

    const stop = Effect.gen(function* () {
      yield* Ref.set(stoppedRef, true);
      const fiber = yield* Ref.get(loopFiberRef);
      yield* Ref.set(loopFiberRef, Option.none());
      if (Option.isSome(fiber)) yield* Fiber.interrupt(fiber.value);
      const active = yield* Ref.get(activeRef);
      if (Option.isSome(active)) yield* Deferred.await(active.value);
    }).pipe(Effect.withSpan("NewestStoriesPoller.stop"));

    const stats = Effect.gen(function* () {
      const [counters, stories, active, stopped] = yield* Effect.all([
        Ref.get(countersRef),
        Ref.get(storiesRef),
        Ref.get(activeRef),
        Ref.get(stoppedRef),
      ]);
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
