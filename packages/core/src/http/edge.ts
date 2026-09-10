import { Deferred, Effect, type Layer } from "effect";
import { HttpEffect, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import type { StreamsFault } from "../fault.ts";
import type { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { program, type HttpOptions } from "./program.ts";

/** Framework conversion boundary; the caller owns disposal for the edge lifetime. */
export const makeEdge = <E, R = never>(
  options: HttpOptions,
  layer: Layer.Layer<StreamsReader | StreamsWriter | R, E>,
  application?: Effect.Effect<
    Response | HttpServerResponse.HttpServerResponse,
    StreamsFault,
    HttpServerRequest.HttpServerRequest | StreamsReader | StreamsWriter | R
  >,
) => {
  const active = new Set<Deferred.Deferred<void>>();
  const effect = Effect.suspend(() => {
    const completed = Deferred.makeUnsafe<void>();
    active.add(completed);
    return Effect.interruptible(application ?? program(options)).pipe(
      Effect.map((response) =>
        response instanceof Response
          ? HttpServerResponse.raw(response, {
              status: response.status,
              statusText: response.statusText,
              headers: Object.fromEntries(response.headers),
            })
          : response,
      ),
      Effect.ensuring(
        Effect.sync(() => active.delete(completed)).pipe(
          Effect.andThen(Deferred.succeed(completed, undefined)),
          Effect.asVoid,
        ),
      ),
    );
  });
  const edge = HttpEffect.toWebHandlerLayer(effect, layer);
  const awaitIdle: Effect.Effect<void> = Effect.suspend(() =>
    active.size === 0
      ? Effect.void
      : Effect.forEach([...active], Deferred.await, { discard: true }).pipe(
          Effect.andThen(awaitIdle),
        ),
  );
  return { ...edge, awaitIdle };
};
