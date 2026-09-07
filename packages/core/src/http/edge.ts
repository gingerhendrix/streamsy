import { Deferred, Effect, type Layer } from "effect";
import { HttpEffect } from "effect/unstable/http";
import type { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { program, type HttpOptions } from "./program.ts";

/** Framework conversion boundary; the caller owns disposal for the edge lifetime. */
export const makeEdge = <E>(
  options: HttpOptions,
  layer: Layer.Layer<StreamsReader | StreamsWriter, E>,
) => {
  const active = new Set<Deferred.Deferred<void>>();
  const application = Effect.suspend(() => {
    const completed = Deferred.makeUnsafe<void>();
    active.add(completed);
    return Effect.interruptible(program(options)).pipe(
      Effect.ensuring(
        Effect.sync(() => active.delete(completed)).pipe(
          Effect.andThen(Deferred.succeed(completed, undefined)),
          Effect.asVoid,
        ),
      ),
    );
  });
  const edge = HttpEffect.toWebHandlerLayer(application, layer);
  const awaitIdle: Effect.Effect<void> = Effect.suspend(() =>
    active.size === 0
      ? Effect.void
      : Effect.forEach([...active], Deferred.await, { discard: true }).pipe(
          Effect.andThen(awaitIdle),
        ),
  );
  return { ...edge, awaitIdle };
};
