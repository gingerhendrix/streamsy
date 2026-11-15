import { Duration, Effect, Option, Pull, Schedule } from "effect";

const defaultSchedule = Schedule.exponential("1 second").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.millis(Math.min(3_600_000, Duration.toMillis(duration)))),
  ),
);

/** Replay timing from the step count; persisted deadlines are never recomputed. */
export const bindingDelay = (schedule: Schedule.Schedule<unknown> | undefined, attempts: number) =>
  Effect.gen(function* () {
    const step = yield* Schedule.toStep(schedule ?? defaultSchedule);
    let delay = 0;

    for (let n = 0; n < attempts; n += 1) {
      const [, duration] = yield* step(0, undefined);
      delay = Duration.toMillis(duration);
    }

    return Option.some(delay);
  }).pipe(
    Pull.catchDone(() => Effect.succeedNone),
    Effect.catchCause((cause) =>
      Effect.logError("Projection binding retry schedule failed", cause).pipe(
        Effect.as(Option.none<number>()),
      ),
    ),
  );
