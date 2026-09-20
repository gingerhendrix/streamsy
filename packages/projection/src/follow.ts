import { Effect, type Fiber, type Scope } from "effect";
import { StreamsReader, ZERO_OFFSET, type StreamRef } from "@streamsy/core";
import type { InputMap } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import type { Fused, Pinned } from "./projection.ts";
import { validateOptions, type RunOptions } from "./read.ts";
import { run, type Host, type Progress } from "./run.ts";

export interface FollowOptions extends RunOptions {
  /** Upper bound on one wake wait; default 1000. */
  readonly repairIntervalMs?: number;
}

/**
 * One wake hint per input; the response is discarded because the next run is
 * authoritative. A closed input answers `readNext` at once, so a closed and
 * drained input parks for the interval instead of waking every cycle; the
 * open inputs, and the interval itself, decide when the next run happens.
 */
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
  const storageFailure = (cause: unknown) =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input,
      message: `Cannot wait on ${ref.id}`,
      cause,
    });
  return yield* reader.readNext(ref.id, { offset }).pipe(
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
          message: `${ref.id} does not support ${error.feature}`,
        }),
      StorageFault: storageFailure,
      TransportFault: storageFailure,
    }),
    Effect.timeoutOption(interval),
    Effect.asVoid,
  );
});

/**
 * Runs to caught-up, then waits for the first input to hint at a change and runs
 * again. The caller's scope owns the fiber and every parked wait; joining exposes
 * the terminal close or a typed failure. On HTTP backends a parked hint is cancelled
 * and replaced once per `repairIntervalMs` until an input changes.
 */
export const follow = Effect.fn("Projection.follow")(function* <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Pinned<Inputs, O, E, R>,
  options: FollowOptions = {},
): Effect.fn.Return<
  Fiber.Fiber<Progress, E | ProjectionFault>,
  ProjectionFault,
  R | Host | Scope.Scope
> {
  yield* validateOptions(options);
  const interval = options.repairIntervalMs ?? 1000;
  if (!Number.isSafeInteger(interval) || interval <= 0)
    return yield* new ProjectionFault({
      phase: "load",
      reason: "invalid-options",
      message: "repairIntervalMs must be a positive safe integer",
    });
  const wake = (result: Progress) =>
    Effect.raceAllFirst(
      Object.entries(projection.inputs).map(([name, ref]) =>
        hint(name, ref, result.record.inputs[name] ?? ZERO_OFFSET, interval),
      ),
    );
  const cycle = run(projection, options).pipe(
    Effect.tap((result) => (result.status === "caught-up" ? wake(result) : Effect.yieldNow)),
  );
  return yield* cycle.pipe(
    Effect.repeat({ while: (result) => result.status !== "source-closed" }),
    Effect.forkScoped,
  );
});
