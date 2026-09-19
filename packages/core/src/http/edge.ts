import { Effect, type Layer } from "effect";
import { HttpEffect, type HttpServerRequest, type HttpServerResponse } from "effect/unstable/http";
import type { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { app, type HttpOptions } from "./program.ts";

/** Framework conversion boundary; the caller owns disposal for the edge lifetime. */
export const makeEdge = <E, R = never>(
  options: HttpOptions,
  layer: Layer.Layer<StreamsReader | StreamsWriter | R, E>,
  application?: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest | StreamsReader | StreamsWriter | R
  >,
) => HttpEffect.toWebHandlerLayer(Effect.interruptible(application ?? app(options)), layer);
