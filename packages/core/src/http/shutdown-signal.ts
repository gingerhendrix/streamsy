import { Context, Deferred, Duration, Effect, Layer, Ref, Schedule, Stream } from "effect";

/**
 * Tells live HTTP reads that the host is about to stop. A parked long poll then
 * answers `204` with its offset and cursor, and an open SSE body ends after its
 * last whole batch. Clients take their normal reconnect path.
 *
 * The service is optional. Live reads look it up with `Effect.serviceOption`, so
 * a host without it (for example the Durable Object host) keeps its behaviour.
 */
export interface ShutdownSignalService {
  /** Completes when the host starts to drain; never fails. */
  readonly draining: Effect.Effect<void>;
  /** Counts a live read while `effect` runs. */
  readonly track: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /** Counts a live read while `stream` runs. */
  readonly trackStream: <A, E, R>(stream: Stream.Stream<A, E, R>) => Stream.Stream<A, E, R>;
  /**
   * Starts the drain, then waits until no live read is running or `timeout`
   * passes. Safe to call more than once.
   */
  readonly drain: (timeout: Duration.Input) => Effect.Effect<void>;
  /** The number of live reads now running. */
  readonly active: Effect.Effect<number>;
}

export class ShutdownSignal extends Context.Service<ShutdownSignal, ShutdownSignalService>()(
  "@streamsy/core/ShutdownSignal",
) {}

/** How often `drain` checks the live-read count. */
const POLL = Schedule.spaced("5 millis");

export const makeShutdownSignal = Effect.gen(function* () {
  const started = yield* Deferred.make<void>();
  const count = yield* Ref.make(0);
  const enter = Ref.update(count, (n) => n + 1);
  const leave = Ref.update(count, (n) => n - 1);
  return ShutdownSignal.of({
    draining: Deferred.await(started),
    track: (effect) =>
      Effect.acquireUseRelease(
        enter,
        () => effect,
        () => leave,
      ),
    trackStream: (stream) =>
      Stream.unwrap(Effect.acquireRelease(enter, () => leave).pipe(Effect.as(stream))),
    drain: (timeout) =>
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(
          Ref.get(count).pipe(Effect.repeat({ until: (n) => n === 0, schedule: POLL })),
        ),
        Effect.timeoutOption(timeout),
        Effect.asVoid,
      ),
    active: Ref.get(count),
  });
});

/** A fresh signal. Provide it to the routes and call `drain` before they close. */
export const layerShutdownSignal = Layer.effect(ShutdownSignal, makeShutdownSignal);
