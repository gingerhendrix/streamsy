import { Protocol } from "@streamsy/core/protocol";
import { Effect, Layer } from "effect";
import { Streamsy, StreamAddress } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";

/** Protocol-focused fixtures use one explicit catch-all stream. */
export const json = <A, I>(
  id: string,
  options: Parameters<typeof StreamAddress.json<A, I, never, never>>[1],
) => Object.assign(StreamAddress.json(id, options), { stream: Streamsy.Wildcard });
export const state = <const C extends StreamAddress.Collections>(
  id: string,
  options: { readonly collections: C & StreamAddress.ValidCollections<C> },
) => Object.assign(StreamAddress.state(id, options), { stream: Streamsy.Wildcard });
export const bytes = (id: string) => Streamsy.Wildcard.at({ id });
export const withFamilies = <A, E, R, const Keys extends ReadonlyArray<Streamsy.Any>>(
  layer: Layer.Layer<A | StreamsReader | StreamsWriter, E, R>,
  ...streams: Keys
) =>
  Protocol.bind(layer, Streamsy.group(...streams, Streamsy.Wildcard)).pipe(
    Layer.provideMerge(layer),
  );

/** Fault injection updates the same scoped wire that projection internals use. */
export const provideWriter =
  (writer: typeof StreamsWriter.Service) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const service = yield* Streamsy.Wildcard;
      return yield* effect.pipe(
        Effect.provideService(Streamsy.Wildcard, {
          ...service,
          wire: { ...service.wire, ...writer },
        }),
        Effect.provideService(StreamsWriter, writer),
      );
    });
export const provideReader =
  (reader: typeof StreamsReader.Service) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const service = yield* Streamsy.Wildcard;
      return yield* effect.pipe(
        Effect.provideService(Streamsy.Wildcard, {
          ...service,
          wire: { ...service.wire, ...reader },
        }),
        Effect.provideService(StreamsReader, reader),
      );
    });
