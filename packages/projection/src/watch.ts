import { Checkpoints } from "./checkpoint.ts";
import type { Outputs } from "./outputs.ts";
import { Effect, Fiber, Queue, Stream as EffectStream, type Scope } from "effect";
import { Streamsy } from "@streamsy/core";
import { inputEntries, type InputServices, type InputMap } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import type { Atomic } from "./projection.ts";
import { validateOptions, type RunOptions } from "./read.ts";
import { run } from "./run.ts";
import type { Progress } from "./run.ts";

export interface WatchOptions extends RunOptions {}

const watchLoop = <Inputs extends InputMap, E, R, OR = never>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
  options: WatchOptions,
): Effect.Effect<
  Progress<Inputs>,
  E | ProjectionFault,
  R | OR | Checkpoints | InputServices<Inputs> | Streamsy.ChangeFeed<InputServices<Inputs>>
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const wake = yield* Queue.make<void>({
        capacity: 1,
        strategy: "dropping",
      });
      const closed = new Set<string>();
      const inputs = inputEntries(projection.inputs);
      yield* Effect.forEach(inputs, ([name, address]) =>
        Effect.gen(function* () {
          const { owner, wire: reader } = yield* address.stream;
          const feed = yield* Streamsy.ChangeFeed(address.stream);
          // Independent Layer overrides can replace a stream without replacing its feed.
          if (feed.owner !== owner)
            return yield* new ProjectionFault({
              phase: "read",
              reason: "unsupported-composition",
              input: name,
              message: `The change feed of ${address.id} belongs to a different owner`,
            });
          yield* reader.head(address.id).pipe(
            Effect.as(true),
            Effect.catchTag("StreamNotFound", () => Effect.succeed(false)),
            Effect.mapError((cause) =>
              cause._tag === "StreamGone"
                ? new ProjectionFault({
                    phase: "read",
                    reason: "history-unavailable",
                    input: name,
                    message: `Required history of ${address.id} is unavailable`,
                    cause,
                  })
                : new ProjectionFault({
                    phase: "read",
                    reason: "storage-failure",
                    input: name,
                    message: `Cannot inspect ${address.id}`,
                    cause,
                  }),
            ),
          );
          return undefined;
        }),
      );
      const subscriptions = inputs.map(([name, address]) =>
        EffectStream.unwrap(
          Effect.map(Streamsy.ChangeFeed(address.stream), (feed) => feed.changes(address.id)),
        ).pipe(
          EffectStream.takeUntil((snapshot) => snapshot.closed),
          EffectStream.runForEach((snapshot) =>
            Effect.gen(function* () {
              if (snapshot.closed) closed.add(name);
              if (snapshot.present) yield* Queue.offer(wake, undefined);
            }),
          ),
          Effect.mapError(
            (cause) =>
              new ProjectionFault({
                phase: "read",
                reason: "storage-failure",
                input: name,
                message: `Cannot watch ${address.id}`,
                cause,
              }),
          ),
          Effect.andThen(Effect.never),
        ),
      );
      const runner = Effect.gen(function* () {
        while (true) {
          yield* Queue.take(wake);
          let result = yield* run(projection, options);
          while (result.status === "limit-reached") result = yield* run(projection, options);
          if (result.status === "source-closed" && closed.size === inputs.length) return result;
        }
      });
      return yield* Effect.raceAllFirst([runner, ...subscriptions]);
    }),
  );

/**
 * Watches each input through its stream change-feed capability.
 * Once watched, an input that is gone surfaces as `read / history-unavailable`; other head
 * failures surface as `read / storage-failure`.
 */
export const watch = <Inputs extends InputMap, E, R, OR = never>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
  options: WatchOptions = {},
): Effect.Effect<
  Fiber.Fiber<Progress<Inputs>, E | ProjectionFault>,
  ProjectionFault,
  | R
  | OR
  | Checkpoints
  | InputServices<Inputs>
  | Streamsy.ChangeFeed<InputServices<Inputs>>
  | Scope.Scope
> =>
  Effect.gen(function* () {
    yield* validateOptions(options);
    return yield* watchLoop(projection, options).pipe(Effect.forkScoped);
  });
