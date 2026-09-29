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

/** Default storage-failure retry for follow; an explicit options.retry takes precedence. */
export const ProjectionRetry = Context.Reference<Schedule.Schedule<unknown, unknown>>(
  "@streamsy/projection/ProjectionRetry",
  { defaultValue: () => defaultRetry },
);
