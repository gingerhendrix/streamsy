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
 */
const attach = <I extends InputMap, E, R, Q, Result, P, OR = never>(
  projection: Attachable<Executable<I, E, R, Q, Result, P, unknown, OR>>,
  options: AttachOptions = {},
) => {
  const runner = runnerOf(projection, options.limit);

  return Effect.gen(function* () {
    const { next, drain } = yield* runner;

    return {
      name: `streamsy/projection/${encodeURIComponent(projection.id)}`,
      next,
      run: drain,
      retry: options.retry ?? (yield* ProjectionRetry),
    };
  });
};

export const ProjectionWakeup = { attach };
