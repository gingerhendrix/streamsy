import { Deferred, Effect, Layer, Option, Ref, Schedule } from "effect";
import { DurableState } from "@streamsy/core";
import { Projection, type Checkpoints, type State } from "@streamsy/projection";
import { newestRows, StoryRows, applyChange } from "../newest-rows.ts";
import { HackerNewsStories, storiesAddress } from "../stories.ts";
import { liveHackerNewsApi } from "../hnews.ts";
import { errorMessage, nowIso } from "../util.ts";
import {
  initialCounters,
  NewestStoriesPoller,
  type NewestStoriesPollerService,
  type PollCounters,
  type PollerConfig,
  type PollStats,
} from "./contract.ts";
import { diffNewest } from "./reconcile.ts";

type PollClaim = {
  readonly owner: boolean;
  readonly gate: Deferred.Deferred<void>;
};

export function makeNewestStoriesPoller(
  config: PollerConfig,
): Effect.Effect<NewestStoriesPollerService, never, HackerNewsStories | Checkpoints | State> {
  return Effect.gen(function* () {
    const api = config.api ?? liveHackerNewsApi;
    const context = yield* Effect.context<HackerNewsStories | Checkpoints | State>();
    const countersRef = yield* Ref.make(initialCounters);
    const activeRef = yield* Ref.make(Option.none<Deferred.Deferred<void>>());

    const patchCounters = (patch: Partial<PollCounters>) =>
      Ref.update(countersRef, (counters) => ({ ...counters, ...patch }));

    const runPoll = Effect.fn("NewestStoriesPoller.runPoll")(function* () {
      yield* patchCounters({
        lastPollStartedAt: yield* nowIso,
        lastPollError: undefined,
        lastUpserts: 0,
        lastDeletes: 0,
      });

      const pass = Effect.gen(function* () {
        const newestIds = [...new Set(yield* api.fetchNewestStoryIds(config.limit))].slice(
          0,
          config.limit,
        );
        // Fetch once; every CAS retry reuses this result and re-reads durable rows.
        const fetched = {
          newestIds: new Set(newestIds),
          stories: yield* api.fetchStoriesById(newestIds),
        };
        for (let attempt = 0; ; attempt++) {
          const outcome = yield* Effect.gen(function* () {
            const progress = yield* Projection.run(newestRows);
            // The initial projection record supplies ZERO_OFFSET for a fresh store.
            const token = progress.record.inputs.input;
            const rows = Option.getOrElse(
              yield* Projection.loadState(newestRows, StoryRows),
              () => ({}),
            );
            const changes = diffNewest(rows, fetched);
            const events = DurableState.changes(storiesAddress, { offset: token }, changes);
            const offset =
              events.length === 0
                ? token
                : (yield* (yield* HackerNewsStories).append({}, events, { expectedOffset: token }))
                    .offset;
            yield* patchCounters({
              lastOffset: offset,
              lastStoryCount: Object.keys(events.reduce(applyChange, rows)).length,
              lastUpserts: changes.filter((change) => change.kind === "upsert").length,
              lastDeletes: changes.filter((change) => change.kind === "delete").length,
            });
            return true;
          }).pipe(
            Effect.catchTag("OffsetMismatch", (error) =>
              attempt === 3
                ? Effect.fail(error)
                : Ref.update(countersRef, (counters) => ({
                    ...counters,
                    totalConflictsRetried: counters.totalConflictsRetried + 1,
                  })).pipe(Effect.as(false)),
            ),
          );
          if (outcome) break;
        }
      });

      yield* pass.pipe(
        Effect.catch((failure) =>
          patchCounters({
            lastPollError:
              failure._tag === "NewestStoriesPoller.PollFailure"
                ? failure.reason
                : `${failure._tag}: ${errorMessage(failure)}`,
          }).pipe(Effect.andThen(Effect.logError("HN poll failed", failure))),
        ),
        Effect.ensuring(
          Effect.flatMap(nowIso, (time) => patchCounters({ lastPollCompletedAt: time })),
        ),
        Effect.provide(context),
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
      const [counters, active] = yield* Effect.all([Ref.get(countersRef), Ref.get(activeRef)]);
      const result: PollStats = {
        polling: Option.isSome(active),
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
