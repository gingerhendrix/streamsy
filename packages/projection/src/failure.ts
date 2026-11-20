import { Array, Cause, Option, Predicate, Schema } from "effect";
import { StorageFault } from "@streamsy/core";
import { NonRetryable, ProjectionFault } from "./fault.ts";
import type { RunnerFailure } from "./registry.ts";

export const nonRetryableHandlerReason = "non-retryable-handler";

const marked = (reason: Cause.Reason<unknown>): boolean =>
  Cause.isFailReason(reason) &&
  Predicate.hasProperty(reason.error, NonRetryable) &&
  reason.error[NonRetryable] === true;

export const hasNonRetryableHandler = (cause: Cause.Cause<unknown>): boolean =>
  Array.some(cause.reasons, marked);

/** Unknown handler errors and defects retry; only a known non-retryable fault stops work. */
export const retryable = (cause: Cause.Cause<unknown>): boolean =>
  !Array.some(cause.reasons, (reason) => {
    if (!Cause.isFailReason(reason)) return false;
    const { error } = reason;

    if (marked(reason)) return true;

    return (
      (Schema.is(ProjectionFault)(error) || Schema.is(StorageFault)(error)) && !error.retryable
    );
  });

export const runnerFailure = (cause: Cause.Cause<unknown>, at: number): RunnerFailure => ({
  at,
  retryable: retryable(cause),
  reason: Cause.findErrorOption(cause).pipe(
    Option.filter(Schema.is(ProjectionFault)),
    Option.map((fault) => fault.reason),
  ),
  message: Cause.pretty(cause).slice(0, 1_000),
});
