import { Array, Effect, Predicate, Result, Schedule } from "effect";
import type { Decided, Decision, OffsetMismatch, Streamsy } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import type { InputMap, ItemOf } from "./batch.ts";
import { current, type CurrentProjection } from "./current.ts";
import type { Snapshot } from "./snapshot.ts";

export interface DecideSpec<Inputs extends InputMap, K extends keyof Inputs, S, B, E, R> {
  /** The input to append to. Its offset in the snapshot is the CAS token. */
  readonly input: K;
  /**
   * Decide what to append from the current value snapshot. This step must not write.
   */
  readonly decide: (
    snapshot: Snapshot<S, Inputs>,
  ) => Effect.Effect<Decision<B, ItemOf<Inputs[K]>>, E, R>;
  /** Retries after `OffsetMismatch`. Default: `Schedule.recurs(3)`. Nothing else is retried. */
  readonly retry?: Schedule.Schedule<unknown, OffsetMismatch>;
}

/** Up to three retries after a lost race, so at most four appends per call. */
const defaultDecideRetry = Schedule.recurs(3);

/**
 * Fold in memory, decide, and append with the snapshot input offset.
 * Retry only this append's OffsetMismatch while the retry schedule allows.
 */
export const decide = Effect.fn("Projection.decide")(function* <
  Inputs extends InputMap,
  const K extends keyof Inputs & string,
  B,
  E,
  R,
  PE,
  PR,
  Q,
  S,
>(projection: CurrentProjection<Inputs, S, PE, PR, Q>, spec: DecideSpec<Inputs, K, S, B, E, R>) {
  // SAFETY: `Inputs[K]` is this input's address; its items are `ItemOf<Inputs[K]>`.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: the selected address and ItemOf share Inputs[K]; TS cannot reduce this conditional generic type.
  const address = projection.inputs[spec.input] as Streamsy.Address<
    ItemOf<Inputs[K]>,
    Inputs[K]["stream"]["Identifier"]
  >;

  const attempt = Effect.gen(function* () {
    const snapshot = yield* current(projection);
    const offset = snapshot.offsets[spec.input];
    const { value, items } = yield* spec.decide(snapshot);

    if (!Array.isReadonlyArrayNonEmpty(items)) {
      const unchanged: Decided<B> = { value, offset, appended: false };

      return unchanged;
    }

    const { wire } = yield* address.stream;

    const appended = yield* Streams.append(address, items, { expectedOffset: offset }).pipe(
      Effect.provideService(StreamsWriter, wire),
      Effect.mapError((error) => (isOffsetMismatch(error) ? new LostRace(error) : error)),
    );

    const decided: Decided<B> = { value, offset: appended.offset, appended: true };

    return decided;
  });

  return yield* retryLostRace(attempt, spec.retry ?? defaultDecideRetry);
});

/**
 * Marks the `OffsetMismatch` of this call's own append. As in `Streams.decide`,
 * only this error is retried: an `OffsetMismatch` that the caller's `decide`
 * raises is its own failure and passes through.
 */
class LostRace {
  constructor(readonly mismatch: OffsetMismatch) {}
}

/**
 * Retry `attempt` after a `LostRace` while `schedule` allows, then fail with the
 * last `OffsetMismatch`. `retry` steps its schedule with every error it sees, so
 * every other outcome passes the retry as a `Result` and is raised after.
 */
const retryLostRace = Effect.fnUntraced(function* <A, E, R>(
  attempt: Effect.Effect<A, E, R>,
  schedule: Schedule.Schedule<unknown, OffsetMismatch>,
) {
  const outcome = yield* attempt.pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        error instanceof LostRace
          ? Effect.fail(error.mismatch)
          : // SAFETY: `instanceof` ruled out `LostRace`; TS cannot narrow a type parameter.
            // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: instanceof excludes LostRace above; TS cannot narrow the generic E to Exclude<E, LostRace>.
            Effect.succeed(Result.fail(error as Exclude<E, LostRace>)),
      onSuccess: (value) => Effect.succeed(Result.succeed(value)),
    }),
    Effect.retry(schedule),
  );

  return yield* Effect.fromResult(outcome);
});

const isOffsetMismatch = <E>(
  error: E | OffsetMismatch,
): error is Extract<E, OffsetMismatch> | OffsetMismatch =>
  Predicate.isTagged(error, "OffsetMismatch");
