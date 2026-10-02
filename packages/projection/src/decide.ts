import { Effect, Predicate, Result, Schedule } from "effect";
import { isReadonlyArrayNonEmpty } from "effect/Array";
import type { Decided, Decision, OffsetMismatch, Streamsy } from "@streamsy/core";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import type { InputMap, ItemOf } from "./batch.ts";
import type { Executable } from "./executable.ts";
import { run } from "./run.ts";
import { read, type Snapshot, type StateValue } from "./snapshot.ts";

export interface DecideSpec<Inputs extends InputMap, K extends keyof Inputs, S, B, E, R> {
  /** The input to append to. Its offset in the snapshot is the CAS token. */
  readonly input: K;
  /**
   * Decide what to append from the snapshot. This step must not write. A
   * projection with no `state:` has `snapshot.state` undefined; read its own tables
   * here instead. They can only be newer than the offsets, and then the append fails
   * and the call runs again.
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
 * Read, decide, append over a projection. Run the projection to its tail, read
 * its state and offsets from one record, decide, and append to `input` with
 * `expectedOffset` set to that input's offset. After an `OffsetMismatch`, run,
 * read and decide again while `retry` allows; then fail with the last
 * `OffsetMismatch`. Transport faults are not retried.
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
  Out,
  P,
  S = never,
  OR = never,
>(
  projection: Executable<Inputs, PE, PR, Q, Out, P, S, OR>,
  spec: DecideSpec<Inputs, K, StateValue<S>, B, E, R>,
) {
  // SAFETY: `Inputs[K]` is this input's address; its items are `ItemOf<Inputs[K]>`.
  const address = projection.inputs[spec.input] as Streamsy.Address<
    ItemOf<Inputs[K]>,
    Inputs[K]["stream"]["Identifier"]
  >;
  const attempt = Effect.gen(function* () {
    // Run with no limit, so the offsets are at the tail.
    yield* run(projection);
    const snapshot = yield* read(projection);
    const offset = snapshot.offsets[spec.input];
    const { value, items } = yield* spec.decide(snapshot);
    if (!isReadonlyArrayNonEmpty(items)) {
      const unchanged: Decided<B> = { value, offset, appended: false };
      return unchanged;
    }
    const { wire } = yield* address.stream;
    const appended = yield* Streams.append(address, items, { expectedOffset: offset }).pipe(
      Effect.provideService(StreamsWriter, wire),
    );
    const decided: Decided<B> = { value, offset: appended.offset, appended: true };
    return decided;
  });
  // As in `Streams.decide`: only a lost race may reach the schedule. Any other
  // outcome passes the retry as a `Result` and is raised after.
  const outcome = yield* attempt.pipe(
    Effect.result,
    Effect.flatMap((result) =>
      Result.isFailure(result) && isOffsetMismatch(result.failure)
        ? Effect.fail(result.failure)
        : Effect.succeed(result),
    ),
    Effect.retry(spec.retry ?? defaultDecideRetry),
  );
  return yield* Effect.fromResult(outcome);
});

const isOffsetMismatch = <E>(
  error: E | OffsetMismatch,
): error is Extract<E, OffsetMismatch> | OffsetMismatch =>
  Predicate.isTagged(error, "OffsetMismatch");
