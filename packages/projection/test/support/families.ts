import { Effect, Layer } from "effect";
import { AnyStream, StreamRef, provides, type StreamFamily } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";

/** Protocol-focused fixtures use one explicit catch-all family. */
export const json = <A, I>(
  id: string,
  options: Parameters<typeof StreamRef.json<A, I, never, never>>[1],
) => Object.assign(StreamRef.json(id, options), { family: AnyStream });
export const bytes = (id: string) => AnyStream.at({ id });
export const withFamilies = <A, E, R, const Keys extends ReadonlyArray<StreamFamily.AnyFamily>>(
  layer: Layer.Layer<A | StreamsReader | StreamsWriter, E, R>,
  ...families: Keys
) => provides(layer, ...families, AnyStream).pipe(Layer.provideMerge(layer));

/** Fault injection updates the same scoped wire that projection internals use. */
export const provideWriter =
  (writer: typeof StreamsWriter.Service) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const service = yield* AnyStream;
      return yield* effect.pipe(
        Effect.provideService(AnyStream, { ...service, wire: { ...service.wire, ...writer } }),
        Effect.provideService(StreamsWriter, writer),
      );
    });
export const provideReader =
  (reader: typeof StreamsReader.Service) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const service = yield* AnyStream;
      return yield* effect.pipe(
        Effect.provideService(AnyStream, { ...service, wire: { ...service.wire, ...reader } }),
        Effect.provideService(StreamsReader, reader),
      );
    });
