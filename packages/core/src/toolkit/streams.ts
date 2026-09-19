import { Effect, Option, Predicate, Schema, Stream } from "effect";
import { DecodeFault, EncodeFault } from "../fault.ts";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import type {
  AppendOptions,
  CreateOptions,
  ReadOptions,
  ReadNextOptions,
} from "../protocol/options.ts";
import type { ReadResult, ReadNextResult } from "../protocol/results.ts";
import type { StreamRef } from "./ref.ts";
export { layerMemory, layerRouted } from "./layers.ts";

export interface Batch<A> {
  readonly items: ReadonlyArray<A>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

export const create = Effect.fn("Streams.create")(function* <A, RD, RE>(
  ref: StreamRef<A, RD, RE>,
  options: Omit<CreateOptions, "contentType"> = {},
) {
  return yield* (yield* StreamsWriter).create(ref.id, { ...options, contentType: ref.contentType });
});
export const append = Effect.fn("Streams.append")(function* <A, RD, RE>(
  ref: StreamRef<A, RD, RE>,
  items: ReadonlyArray<A>,
  options: Omit<AppendOptions, "data" | "contentType"> = {},
) {
  const encoded = yield* Effect.forEach(items, (item) =>
    Schema.encodeEffect(ref.codec)(item).pipe(
      Effect.mapError((cause) => new EncodeFault({ message: `Cannot encode ${ref.id}`, cause })),
    ),
  );
  const data =
    items.length === 0 && options.close
      ? // A close-only append is the protocol's empty body. An empty JSON array is
        // a malformed append on every transport, so it must not carry the close.
        new Uint8Array()
      : Predicate.isTagged(ref, "Json")
        ? new TextEncoder().encode(`[${encoded.join(",")}]`)
        : concatBytes(encoded);
  return yield* (yield* StreamsWriter).append(ref.id, {
    ...options,
    data,
    contentType: ref.contentType,
  });
});
function concatBytes(parts: ReadonlyArray<string | Uint8Array>): Uint8Array {
  const arrays = parts.map((part) =>
    Predicate.isString(part) ? new TextEncoder().encode(part) : part,
  );
  const result = new Uint8Array(arrays.reduce((size, part) => size + part.byteLength, 0));
  let offset = 0;
  for (const part of arrays) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}
const decode = Effect.fn("Streams.decode")(function* <A, RD, RE>(
  ref: StreamRef<A, RD, RE>,
  result: ReadResult | ReadNextResult,
) {
  const items = yield* Effect.forEach(result.messages, (message, index) =>
    Schema.decodeEffect(ref.codec)(
      Predicate.isTagged(ref, "Json") ? new TextDecoder().decode(message.data) : message.data,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new DecodeFault({
            message: `Cannot decode ${ref.id} at read message ${index}`,
            cause,
          }),
      ),
    ),
  );
  return {
    items,
    nextOffset: result.nextOffset,
    upToDate: result.upToDate,
    closed: result.closed === true,
  } satisfies Batch<A>;
});
export function read<A, RD, RE>(ref: StreamRef<A, RD, RE>, options: ReadOptions = {}) {
  return Stream.unwrap(
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      return Stream.paginate(options.offset, (offset) =>
        Effect.gen(function* () {
          const batch = yield* decode(ref, yield* reader.read(ref.id, { ...options, offset }));
          return [[batch], batch.upToDate ? Option.none() : Option.some(batch.nextOffset)] as const;
        }),
      );
    }),
  );
}
/** Catch up, then wait one readNext at a time, filtering empty open batches. */
export function follow<A, RD, RE>(ref: StreamRef<A, RD, RE>, options: ReadOptions = {}) {
  return Stream.unwrap(
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      return Stream.paginate({ offset: options.offset, live: false }, (state) =>
        Effect.gen(function* () {
          const batch = yield* decode(
            ref,
            state.live
              ? yield* reader.readNext(ref.id, { offset: state.offset ?? "-1" })
              : yield* reader.read(ref.id, { ...options, offset: state.offset }),
          );
          return [
            batch.items.length > 0 || batch.closed ? [batch] : [],
            batch.closed
              ? Option.none()
              : Option.some({ offset: batch.nextOffset, live: batch.upToDate }),
          ] as const;
        }),
      );
    }),
  );
}
export const items = <A, E, R>(batches: Stream.Stream<Batch<A>, E, R>): Stream.Stream<A, E, R> =>
  batches.pipe(Stream.flatMap((batch) => Stream.fromIterable(batch.items)));
export const session = Effect.fn("Streams.session")(function* <A, RD, RE>(
  ref: StreamRef<A, RD, RE>,
  options: ReadNextOptions,
) {
  return yield* (yield* StreamsReader).readNext(ref.id, options);
});
export const head = Effect.fn("Streams.head")(function* <A, RD, RE>(ref: StreamRef<A, RD, RE>) {
  return yield* (yield* StreamsReader).head(ref.id);
});
export const remove = Effect.fn("Streams.remove")(function* <A, RD, RE>(ref: StreamRef<A, RD, RE>) {
  return yield* (yield* StreamsWriter).remove(ref.id);
});
