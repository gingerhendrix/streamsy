import { Array, Cause, Option, Schema } from "effect";
import { StorageFault } from "@streamsy/core";
import { ProjectionFault } from "./fault.ts";
import type { RunnerFailure } from "./registry.ts";

/** Unknown handler errors and defects retry; only a known non-retryable fault stops work. */
export const retryable = (cause: Cause.Cause<unknown>): boolean =>
  !Array.some(cause.reasons, (reason) =>
    Cause.isFailReason(reason) &&
    (Schema.is(ProjectionFault)(reason.error) || Schema.is(StorageFault)(reason.error))
      ? !reason.error.retryable
      : false,
  );

export const runnerFailure = (cause: Cause.Cause<unknown>, at: number): RunnerFailure => ({
  at,
  retryable: retryable(cause),
  reason: Cause.findErrorOption(cause).pipe(
    Option.filter(Schema.is(ProjectionFault)),
    Option.map((fault) => fault.reason),
  ),
  message: Cause.pretty(cause).slice(0, 1_000),
});
