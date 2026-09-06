import { Effect, type Layer } from "effect";
import { HttpEffect } from "effect/unstable/http";
import type { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import { program, type HttpOptions } from "./program.ts";

/** Framework conversion boundary; the caller owns disposal for the edge lifetime. */
export const makeEdge = <E>(
  options: HttpOptions,
  layer: Layer.Layer<StreamsReader | StreamsWriter, E>,
) =>
  // rc.112 masks the handled request; restore cancellation for application work
  // while leaving response delivery and scope finalization owned by HttpEffect.
  HttpEffect.toWebHandlerLayer(Effect.interruptible(program(options)), layer);
