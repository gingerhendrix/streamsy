import type { LazyArg } from "effect/Function";
import { Duration, Effect, Option, Predicate, Schedule, Schema, Stream } from "effect";
import { DecodeFault, EncodeFault } from "../fault.ts";
import { ZERO_OFFSET } from "../offset/index.ts";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import type {
  AppendOptions,
  CreateOptions,
  ReadOptions,
  ReadNextOptions,
} from "../protocol/options.ts";
import type { ReadResult, ReadNextResult } from "../protocol/results.ts";
import type { StreamAddress } from "./address.ts";
export { layerMemory } from "./layers.ts";

export interface Batch<A> {
  readonly items: ReadonlyArray<A>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

export const create = Effect.fn("Streams.create")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  options: Omit<CreateOptions, "contentType"> = {},
) {
  return yield* (yield* StreamsWriter).create(address.id, {
    ...options,
    contentType: address.contentType,
  });
});
export const append = Effect.fn("Streams.append")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  items: ReadonlyArray<A>,
  options: Omit<AppendOptions, "data" | "contentType"> = {},
) {
  const encoded = yield* Effect.forEach(items, (item) =>
    Schema.encodeEffect(address.codec)(item).pipe(
      Effect.mapError(
        (cause) => new EncodeFault({ message: `Cannot encode ${address.id}`, cause }),
      ),
    ),
  );
  const data =
    items.length === 0 && options.close
      ? // A close-only append is the protocol's empty body. An empty JSON array is
        // a malformed append on every transport, so it must not carry the close.
        new Uint8Array()
      : Predicate.isTagged(address, "Json")
        ? new TextEncoder().encode(`[${encoded.join(",")}]`)
        : concatBytes(encoded);
  return yield* (yield* StreamsWriter).append(address.id, {
    ...options,
    data,
    contentType: address.contentType,
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
  address: StreamAddress<A, RD, RE>,
  result: ReadResult | ReadNextResult,
) {
  const items = yield* Effect.forEach(result.messages, (message, index) =>
    Schema.decodeEffect(address.codec)(
      Predicate.isTagged(address, "Json") ? new TextDecoder().decode(message.data) : message.data,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new DecodeFault({
            message: `Cannot decode ${address.id} at read message ${index}`,
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
export function read<A, RD, RE>(address: StreamAddress<A, RD, RE>, options: ReadOptions = {}) {
  return Stream.unwrap(
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      return Stream.paginate(options.offset, (offset) =>
        Effect.gen(function* () {
          const batch = yield* decode(
            address,
            yield* reader.read(address.id, { ...options, offset }),
          );
          return [[batch], batch.upToDate ? Option.none() : Option.some(batch.nextOffset)] as const;
        }),
      );
    }),
  );
}
/** A folded value and the tail it was read up to. `offset` is this stream's CAS token. */
export interface Folded<S> {
  readonly state: S;
  readonly offset: string;
}
/** Every item read and the tail they were read up to. */
export interface Collected<A> {
  readonly items: ReadonlyArray<A>;
  readonly offset: string;
}
/**
 * Read the stream up to its tail and fold every item into one value. Pass
 * `options.offset` to fold on from a tail that an earlier fold returned.
 */
export const fold = Effect.fn("Streams.fold")(function* <A, RD, RE, S>(
  address: StreamAddress<A, RD, RE>,
  initial: LazyArg<S>,
  f: (state: S, item: A) => S,
  options: ReadOptions = {},
) {
  return yield* read(address, options).pipe(
    Stream.runFold(
      // The first page always replaces this offset, so it never reaches the caller.
      (): Folded<S> => ({ state: initial(), offset: options.offset ?? ZERO_OFFSET }),
      (folded, batch): Folded<S> => {
        let state = folded.state;
        for (const item of batch.items) state = f(state, item);
        return { state, offset: batch.nextOffset };
      },
    ),
  );
});
/** Read the stream up to its tail and return its items. */
export const collect = Effect.fn("Streams.collect")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  options: ReadOptions = {},
) {
  const { state, offset } = yield* fold(
    address,
    (): Array<A> => [],
    (items: Array<A>, item: A) => {
      items.push(item);
      return items;
    },
    options,
  );
  const collected: Collected<A> = { items: state, offset };
  return collected;
});
export interface FollowOptions extends ReadOptions {
  /**
   * Schedule for retrying a failed read after a `TransportFault` or `StorageFault`.
   * Default: exponential from 200 ms, jittered, capped at 30 s, forever. Other
   * failures end the stream at once.
   */
  readonly retry?: Schedule.Schedule<unknown, unknown>;
}

const capAt30Seconds = Schedule.modifyDelay(({ duration }: { duration: Duration.Duration }) =>
  Effect.succeed(Duration.millis(Math.min(30_000, Duration.toMillis(duration)))),
);
/** Same shape as `Projection.follow`'s default; kept as a copy until a third user appears. */
const defaultFollowRetry = Schedule.exponential("200 millis").pipe(
  capAt30Seconds,
  Schedule.jittered,
  // Jitter ranges up to 1.2; clamp again so actual waits never exceed 30 s.
  capAt30Seconds,
);
const isStreamsFault = (error: { readonly _tag: string }) =>
  Predicate.isTagged(error, "TransportFault") || Predicate.isTagged(error, "StorageFault");

interface FollowState {
  readonly offset: string | undefined;
  readonly live: boolean;
  /** Last `Stream-Cursor` from a live read; echoed on the next one for CDN collapsing. */
  readonly cursor: string | undefined;
}
/**
 * Catch up, then wait one readNext at a time, filtering empty open batches.
 * A read that fails with a transport or storage fault is retried from the same
 * offset, so a restart or network drop yields each item once.
 */
export function follow<A, RD, RE>(address: StreamAddress<A, RD, RE>, options: FollowOptions = {}) {
  const { retry = defaultFollowRetry, ...readOptions } = options;
  const retrying = <T, E extends { readonly _tag: string }, R>(read: Effect.Effect<T, E, R>) =>
    read.pipe(
      Effect.tapError((error) =>
        isStreamsFault(error) ? Effect.logWarning("Streams.follow retry", error) : Effect.void,
      ),
      Effect.retry({ while: isStreamsFault, schedule: retry }),
    );
  return Stream.unwrap(
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      const initial: FollowState = { offset: readOptions.offset, live: false, cursor: undefined };
      const live = (state: FollowState) =>
        reader
          .readNext(address.id, { offset: state.offset ?? "-1", cursor: state.cursor })
          .pipe(Effect.map((result) => [result, result.cursor] as const));
      const catchUp = (state: FollowState) =>
        reader
          .read(address.id, { ...readOptions, offset: state.offset })
          .pipe(Effect.map((result) => [result, state.cursor] as const));
      return Stream.paginate(initial, (state) =>
        Effect.gen(function* () {
          // Retry only this step's read. The offset in `state` is kept, so a retried
          // read returns nothing the stream has already yielded.
          const [result, cursor] = yield* retrying(state.live ? live(state) : catchUp(state));
          const batch = yield* decode(address, result);
          const next: FollowState = { offset: batch.nextOffset, live: batch.upToDate, cursor };
          return [
            batch.items.length > 0 || batch.closed ? [batch] : [],
            batch.closed ? Option.none() : Option.some(next),
          ] as const;
        }),
      );
    }),
  );
}
export const items = <A, E, R>(batches: Stream.Stream<Batch<A>, E, R>): Stream.Stream<A, E, R> =>
  batches.pipe(Stream.flatMap((batch) => Stream.fromIterable(batch.items)));
export const session = Effect.fn("Streams.session")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  options: ReadNextOptions,
) {
  return yield* (yield* StreamsReader).readNext(address.id, options);
});
export const head = Effect.fn("Streams.head")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
) {
  return yield* (yield* StreamsReader).head(address.id);
});
export const remove = Effect.fn("Streams.remove")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
) {
  return yield* (yield* StreamsWriter).remove(address.id);
});
