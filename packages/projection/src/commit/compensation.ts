import { type Cause, Effect, Function, Option } from "effect";
import { make, type Commit, type HandlerFailed } from "./model.ts";
import type { ProjectionFault } from "../fault.ts";
import type { Unit } from "../unit.ts";
import { detailsOf, setDetails, inherit } from "./internal.ts";

export const withCompensation: {
  <Result, R>(
    f: (
      result: Result,
      cause: Cause.Cause<ProjectionFault | HandlerFailed>,
      unit: Unit,
    ) => Effect.Effect<void, never, R>,
  ): <P, Q>(self: Commit<Result, P, Q>) => Commit<Result, P, Q | R>;
  <Result, P, Q, R>(
    self: Commit<Result, P, Q>,
    f: (
      result: Result,
      cause: Cause.Cause<ProjectionFault | HandlerFailed>,
      unit: Unit,
    ) => Effect.Effect<void, never, R>,
  ): Commit<Result, P, Q | R>;
} = Function.dual(
  2,
  <Result, P, Q, R>(
    self: Commit<Result, P, Q>,
    f: (
      result: Result,
      cause: Cause.Cause<ProjectionFault | HandlerFailed>,
      unit: Unit,
    ) => Effect.Effect<void, never, R>,
  ): Commit<Result, P, Q | R> =>
    inherit(
      self,
      make({
        ...self,
        apply: (pending) =>
          Effect.suspend(() => {
            let completed = Option.none<Result>();

            const observed = setDetails(
              {
                ...pending,
                run: pending.run.pipe(
                  Effect.tap((result) =>
                    Effect.sync(() => {
                      completed = Option.some(result);
                    }),
                  ),
                ),
              },
              detailsOf(pending),
            );

            return self
              .apply(observed)
              .pipe(
                Effect.catchCause((cause) =>
                  (Option.isSome(completed)
                    ? f(completed.value, cause, pending.unit)
                    : Effect.void
                  ).pipe(Effect.andThen(Effect.failCause(cause))),
                ),
              );
          }),
      }),
    ),
);
