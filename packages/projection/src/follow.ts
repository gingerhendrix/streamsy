import { ProjectionRetry } from "./retry.ts";
import type { Executable } from "./executable.ts";
import { Array, Schema, Schedule, Effect, Ref, Stream } from "effect";
import { ZERO_OFFSET, type Streamsy } from "@streamsy/core";
import { inputEntries, type InputServices, type InputMap } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import { validateOptions, type RunOptions } from "./read.ts";
import { catchUp, progressOf, type Cycle, type Progress } from "./run.ts";
import type { Offsets, StateValue } from "./snapshot.ts";

export interface FollowOptions extends RunOptions {
  /** Upper bound on one wake wait; default 30000. */
  readonly repairIntervalMs?: number;
  /** Override the forever, capped exponential retry for retryable storage failures. */
  readonly retry?: Schedule.Schedule<unknown>;
}

/**
 * One wake hint per input; the messages are discarded because the next run is
 * authoritative. Only the response cursor is kept, so the next hint on that
 * input echoes it. A closed input answers `readNext` at once, so a closed and
 * drained input parks for the interval instead of waking every cycle; the
 * open inputs, and the interval itself, decide when the next run happens.
 */
const hint = Effect.fn("Projection.wake")(function* <R>(
  input: string,
  address: Streamsy.Address<unknown, R>,
  offset: string,
  interval: number,
  cursors: Ref.Ref<Readonly<Record<string, string>>>,
) {
  const cursor = (yield* Ref.get(cursors))[input];
  const { wire: reader } = yield* address.stream;

  const historyUnavailable = () =>
    new ProjectionFault({
      phase: "read",
      reason: "history-unavailable",
      input,
      message: `Required history of ${address.id} after ${offset} is unavailable`,
    });

  const storageFailure = (cause: unknown) =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input,
      message: `Cannot wait on ${address.id}`,
      cause,
    });

  return yield* reader.readNext(address.id, { offset, cursor }).pipe(
    Effect.tap((result) =>
      Ref.update(cursors, (current) => ({ ...current, [input]: result.cursor })),
    ),
    Effect.flatMap((result) =>
      result.closed && result.messages.length === 0 ? Effect.never : Effect.void,
    ),
    Effect.catchTags({
      InvalidReadRequest: (cause) =>
        new ProjectionFault({
          phase: "read",
          reason: "invalid-source",
          input,
          message: cause.message,
          cause,
        }),
      StreamNotFound: () => historyUnavailable(),
      StreamGone: () => historyUnavailable(),
      NotSupported: (error) =>
        new ProjectionFault({
          phase: "read",
          reason: "unsupported-composition",
          input,
          message: `${address.id} does not support ${error.feature}`,
        }),
      StorageFault: storageFailure,
      TransportFault: storageFailure,
    }),
    Effect.timeoutOption(interval),
    Effect.asVoid,
  );
});

/**
 * Follows a projection as a pull stream. Each element is one committed cycle: the
 * state and offsets that the last pass left, plus the units and items of the cycle.
 *
 * To run the stream is to be the writer, so run one `follow` stream per key. The next
 * cycle runs only when the consumer pulls, so a slow consumer gets one element that
 * covers all the input that arrived meanwhile. The stream emits after the first cycle,
 * after each cycle that commits a unit or reaches `limit`, and after the last cycle
 * when every input is closed; then it ends. A wake that finds no input emits nothing.
 * Options are checked at the first pull. On HTTP backends a parked hint is cancelled
 * and replaced once per `repairIntervalMs` until an input changes. One
 * `Projection.follow` span covers the whole stream.
 */
export const follow = <Inputs extends InputMap, E, R, Q, Result, P, S = never, OR = never>(
  projection: Executable<Inputs, E, R, Q, Result, P, S, OR>,
  options: FollowOptions = {},
): Stream.Stream<
  Progress<StateValue<S>, Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> =>
  Stream.unwrap(
    Effect.gen(function* () {
      yield* validateOptions(options);
      const interval = options.repairIntervalMs ?? 30_000;

      if (!Number.isSafeInteger(interval) || interval <= 0)
        return yield* new ProjectionFault({
          phase: "load",
          reason: "invalid-options",
          message: "repairIntervalMs must be a positive safe integer",
        });
      const cursors = yield* Ref.make<Readonly<Record<string, string>>>({});

      const schedule = (options.retry ?? (yield* ProjectionRetry)).pipe(
        Schedule.tap(({ input }) =>
          Schema.is(ProjectionFault)(input) && input.retryable
            ? Effect.logWarning("Projection.follow retry", input)
            : Effect.void,
        ),
      );

      const retrying = <A, EA, RA>(effect: Effect.Effect<A, EA, RA>) =>
        Effect.retry(effect, {
          while: (fault) => Schema.is(ProjectionFault)(fault) && fault.retryable,
          schedule,
        });

      const wake = (offsets: Offsets<Inputs>) =>
        Effect.raceAllFirst(
          Array.map(inputEntries(projection.inputs), ([name, address]) =>
            hint(name, address, offsets[name] ?? ZERO_OFFSET, interval, cursors),
          ),
        );

      /**
       * One cycle, retried on a retryable fault. When it follows a caught-up cycle it
       * first waits for a hint; a retry after a failed wait runs at once.
       */
      const cycle = (previous: Cycle<Inputs> | undefined) => {
        let waiting = previous?.status === "caught-up";

        return retrying(
          Effect.suspend(() => {
            const parked = waiting && previous !== undefined;
            waiting = false;

            return parked
              ? wake(previous.offsets).pipe(Effect.andThen(catchUp(projection, options)))
              : catchUp(projection, options);
          }),
        );
      };

      /** Runs cycles until one must be emitted: the first, a commit, a limit, or the close. */
      const next = Effect.fnUntraced(function* (previous: Cycle<Inputs> | undefined) {
        let result = yield* cycle(previous);

        if (previous === undefined) return result;

        while (result.status === "caught-up" && result.units === 0) result = yield* cycle(result);

        return result;
      });

      return Stream.unfold<
        Cycle<Inputs> | undefined,
        Progress<StateValue<S>, Inputs>,
        E | ProjectionFault,
        Exclude<R, P> | Q | OR | InputServices<Inputs>
      >(undefined, (previous) =>
        Effect.gen(function* () {
          if (previous?.status === "source-closed") return undefined;
          const result = yield* next(previous);

          return [yield* progressOf(projection, result), result] as const;
        }),
      );
    }),
  ).pipe(Stream.withSpan("Projection.follow"));
