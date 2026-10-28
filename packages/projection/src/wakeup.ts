import { Effect } from "effect";
import type { InputMap } from "./batch.ts";
import type { Executable } from "./executable.ts";
import { runnerOf, type Attachable } from "./runner.ts";
import { ProjectionRetry } from "./retry.ts";
import type { AttachOptions } from "./attach.ts";

/**
 * Local commits choose bindings; each alarm drains the same runner used by
 * Projection.attach. Acquire it in the object layer's scope, before any
 * request writes.
 *
 * Durable by default: each commit writes a pending mark for its bindings in
 * the same transaction, so an eviction or a deploy between the commit and the
 * alarm loses no work. The host's acquire reconcile reads the marks and arms
 * the alarm. Pass `durable: false` for the in-memory set only.
 */
const attach = <I extends InputMap, E, R, Q, Result, P, OR = never>(
  projection: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>>,
  options: AttachOptions = {},
) => {
  const runner = runnerOf(projection, {
    limit: options.limit,
    durable: options.durable ?? true,
    batch: options.batch,
  });

  return Effect.gen(function* () {
    const { next, drain, deferred } = yield* runner;

    return {
      name: `streamsy/projection/${encodeURIComponent(projection.id)}`,
      next,
      run: drain,
      retry: options.retry ?? (yield* ProjectionRetry),
      deferred,
    };
  });
};

export const ProjectionWakeup = { attach };
