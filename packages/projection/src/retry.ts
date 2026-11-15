import { Context, Duration, Effect, Schedule } from "effect";

const defaultRetry = Schedule.exponential("200 millis").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.millis(Math.min(30_000, Duration.toMillis(duration)))),
  ),
  Schedule.jittered,
  // Jitter ranges up to 1.2; clamp again so actual waits never exceed 30 s.
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.millis(Math.min(30_000, Duration.toMillis(duration)))),
  ),
);

/**
 * Default retry for following and attached runners; an explicit options.retry takes precedence.
 * For attached runners: timing only. The runner calls the schedule once per retryable failed pass with no input.
 */
export const ProjectionRetry = Context.Reference<Schedule.Schedule<unknown>>(
  "@streamsy/projection/ProjectionRetry",
  { defaultValue: () => defaultRetry },
);
