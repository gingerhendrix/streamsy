import { Effect, type Fiber, type Scope } from "effect";
import { StreamsReader, ZERO_OFFSET, type StreamRef } from "@streamsy/core";
import type { InputMap } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import type { Fused, Stream } from "./projection.ts";
import { validateBudget, type Budget } from "./read.ts";
import { run, type Host, type Progress } from "./run.ts";

export interface FollowOptions extends Budget {
  /** Upper bound on one wake wait and the pace of an oversized unit; default 1000. */
  readonly repairIntervalMs?: number;
}

/** One wake hint per input; the response is discarded because the next run is authoritative. */
const hint = Effect.fn("Projection.wake")(function* (
  input: string,
  ref: StreamRef.StreamRef<unknown>,
  offset: string,
  interval: number,
) {
  const reader = yield* StreamsReader;
  const historyUnavailable = () =>
    new ProjectionFault({
      phase: "read",
      reason: "history-unavailable",
      input,
      message: `Required history of ${ref.id} after ${offset} is unavailable`,
    });
  const storageFailure = () =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input,
      message: `Cannot wait on ${ref.id}`,
    });
  return yield* reader.readNext(ref.id, { offset }).pipe(
    Effect.asVoid,
    Effect.catchTags({
      StreamNotFound: () => historyUnavailable(),
      StreamGone: () => historyUnavailable(),
      NotSupported: (error) =>
        new ProjectionFault({
          phase: "read",
          reason: "unsupported-composition",
          input,
          message: `${ref.id} does not support ${error.feature}`,
        }),
      StorageFault: () => storageFailure(),
      TransportFault: () => storageFailure(),
    }),
    Effect.timeoutOption(interval),
    Effect.asVoid,
  );
});

/**
 * Runs to caught-up, then waits for the first input to hint at a change and runs
 * again. The caller's scope owns the fiber and every parked wait; joining exposes
 * the terminal close or a typed failure. On HTTP backends each hint is one long poll.
 */
export const follow = Effect.fn("Projection.follow")(function* <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Stream<Inputs, O, E, R>,
  options: FollowOptions = {},
): Effect.fn.Return<
  Fiber.Fiber<Progress, E | ProjectionFault>,
  ProjectionFault,
  R | Host | Scope.Scope
> {
  yield* validateBudget(options);
  const interval = options.repairIntervalMs ?? 1000;
  if (!Number.isFinite(interval) || interval <= 0)
    return yield* new ProjectionFault({
      phase: "load",
      reason: "invalid-budget",
      message: "repairIntervalMs must be positive",
    });
  const wake = (result: Progress) =>
    Effect.raceAllFirst(
      Object.entries(projection.inputs).map(([name, ref]) =>
        hint(name, ref, result.record.inputs[name] ?? ZERO_OFFSET, interval),
      ),
    );
  const cycle = run(projection, options).pipe(
    Effect.tap((result) =>
      result.status === "caught-up"
        ? wake(result)
        : result.status === "limit-reached" && result.units === 0
          ? Effect.sleep(interval)
          : Effect.yieldNow,
    ),
  );
  return yield* cycle.pipe(
    Effect.repeat({ while: (result) => result.status !== "source-closed" }),
    Effect.forkScoped,
  );
});
