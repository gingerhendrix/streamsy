import { isReadonlyArrayNonEmpty, type NonEmptyReadonlyArray } from "effect/Array";
import type { LazyArg } from "effect/Function";
import { Duration, Effect, Option, Predicate, Result, Schedule, Schema, Stream } from "effect";
import { DecodeFault, EncodeFault, isRetryableFault, type StreamsFault } from "../fault.ts";
import { ZERO_OFFSET } from "../offset/index.ts";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import type {
  AppendError,
  AppendErrorFor,
  OffsetMismatch,
  StreamNotFound,
  StreamGone,
  InvalidReadRequest,
  NotSupported,
  CreateError,
  HeadError,
  RemoveError,
} from "../protocol/errors.ts";
import type {
  AppendCallOptions,
  CreateOptions,
  ReadOptions,
  ReadNextOptions,
} from "../protocol/options.ts";
import type {
  AppendResult,
  CreateResult,
  HeadResult,
  ReadResult,
  ReadNextResult,
} from "../protocol/results.ts";
import type { StreamAddress } from "./address.ts";

/** Full toolkit failures, including protocol rejections and infrastructure faults. */
export type ReadFailure =
  | StreamNotFound
  | StreamGone
  | InvalidReadRequest
  | StreamsFault
  | DecodeFault;
export type FollowFailure = ReadFailure | NotSupported;
export type AppendFailure<O = AppendCallOptions> = AppendErrorFor<O> | StreamsFault | EncodeFault;
export type CreateFailure = CreateError | StreamsFault;
export type HeadFailure = HeadError | StreamsFault;
export type RemoveFailure = RemoveError | StreamsFault;

export interface Batch<A> {
  readonly items: ReadonlyArray<A>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

export const create = Effect.fn("Streams.create")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  options: Omit<CreateOptions, "contentType"> = {},
): Effect.fn.Return<CreateResult, CreateFailure, StreamsWriter> {
  return yield* (yield* StreamsWriter).create(address.id, {
    ...options,
    contentType: address.contentType,
  });
});
/**
 * The items one append takes. An append carries at least one item, unless it
 * closes the stream. Caller-bug rejections from a host are defects.
 */
export type AppendItems<A, O> = O extends { readonly close: true }
  ? ReadonlyArray<A>
  : NonEmptyReadonlyArray<A>;
/** Whether these options can lead to this rejection. Any other rejection is a host bug. */
const possibleFor = (options: AppendCallOptions, error: AppendError | StreamsFault): boolean =>
  Predicate.isTagged(error, "InvalidAppendRequest") || Predicate.isTagged(error, "InvalidEpochSeq")
    ? false
    : Predicate.isTagged(error, "OffsetMismatch")
      ? options.expectedOffset !== undefined
      : Predicate.isTagged(error, "StaleEpoch") || Predicate.isTagged(error, "ProducerGap")
        ? options.producer !== undefined
        : true;
/**
 * Append items to one stream. The error type lists only the rejections that the
 * options make possible; a host that answers another one dies.
 */
export const append = Effect.fn("Streams.append")(function* <
  A,
  RD,
  RE,
  const O extends AppendCallOptions = {},
>(
  address: StreamAddress<A, RD, RE>,
  items: AppendItems<A, O>,
  options?: O,
): Effect.fn.Return<AppendResult, AppendFailure<O>, StreamsWriter | RE> {
  const callOptions: AppendCallOptions = options ?? {};
  const list: ReadonlyArray<A> = items;
  const encoded = yield* Effect.forEach(list, (item) =>
    Schema.encodeEffect(address.codec)(item).pipe(
      Effect.mapError(
        (cause) => new EncodeFault({ message: `Cannot encode ${address.id}`, cause }),
      ),
    ),
  );
  for (const item of encoded) {
    if (Predicate.isUint8Array(item) && item.byteLength === 0)
      return yield* Effect.die(
        new RangeError(`Cannot append an empty bytes item to ${address.id}`),
      );
  }
  const data =
    list.length === 0 && callOptions.close
      ? // A close-only append is the protocol's empty body. An empty JSON array is
        // a malformed append on every transport, so it must not carry the close.
        new Uint8Array()
      : Predicate.isTagged(address, "Json")
        ? new TextEncoder().encode(`[${encoded.join(",")}]`)
        : concatBytes(encoded);
  const possible = (error: AppendError | StreamsFault): error is AppendErrorFor<O> | StreamsFault =>
    possibleFor(callOptions, error);
  return yield* (yield* StreamsWriter)
    .append(address.id, { ...callOptions, data, contentType: address.contentType })
    .pipe(Effect.catch((error) => (possible(error) ? Effect.fail(error) : Effect.die(error))));
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
export function read<A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  options: ReadOptions = {},
): Stream.Stream<Batch<A>, ReadFailure, StreamsReader | RD> {
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
): Effect.fn.Return<Folded<S>, ReadFailure, StreamsReader | RD> {
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
): Effect.fn.Return<Collected<A>, ReadFailure, StreamsReader | RD> {
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
/** What one `decide` step answers: a value, and the items to append for it. */
export interface Decision<B, A> {
  readonly value: B;
  /** Items to append. Empty means "no write": the call returns without a tail check. */
  readonly items: ReadonlyArray<A>;
}
/** The result of a read, decide, append loop. */
export interface Decided<B> {
  readonly value: B;
  /** The new tail after a write, or the tail that was read when there was no write. */
  readonly offset: string;
  readonly appended: boolean;
}
export interface DecideSpec<A, S, B, E, R> {
  /** The start value of the fold. Each call starts from a new value. */
  readonly initial: LazyArg<S>;
  readonly fold: (state: S, item: A) => S;
  /**
   * Decide what to append from the folded state. `offset` is the tail the state is
   * at; the append checks it. This step must not write and must not change
   * `state`: a retry folds on from the same state after a lost race.
   */
  readonly decide: (
    state: S,
    at: { readonly offset: string },
  ) => Effect.Effect<Decision<B, NoInfer<A>>, E, R>;
  /** Retries after `OffsetMismatch`. Default: `Schedule.recurs(3)`. Nothing else is retried. */
  readonly retry?: Schedule.Schedule<unknown, OffsetMismatch>;
}
/** Up to three retries after a lost race, so at most four appends per call. */
const defaultDecideRetry = Schedule.recurs(3);
/**
 * Read, decide, append. Fold the stream to its tail, decide from the state, and
 * append with `expectedOffset` set to the tail that was read. After an
 * `OffsetMismatch`, fold on from that tail and decide again while `retry` allows;
 * then fail with the last `OffsetMismatch`. Transport faults are not retried: a
 * lost reply may hide an append that landed.
 */
export const decide = Effect.fn("Streams.decide")(function* <A, RD, RE, S, B, E, R>(
  address: StreamAddress<A, RD, RE>,
  spec: DecideSpec<A, S, B, E, R>,
): Effect.fn.Return<
  Decided<B>,
  ReadFailure | AppendFailure<{ expectedOffset: string }> | E,
  StreamsReader | StreamsWriter | RD | RE | R
> {
  let folded: Folded<S> | undefined;
  const attempt = Effect.gen(function* () {
    const last = folded;
    folded =
      last === undefined
        ? yield* fold(address, spec.initial, spec.fold)
        : yield* fold(address, () => last.state, spec.fold, { offset: last.offset });
    const { state, offset } = folded;
    const { value, items } = yield* spec.decide(state, { offset });
    if (!isReadonlyArrayNonEmpty(items)) {
      const unchanged: Decided<B> = { value, offset, appended: false };
      return unchanged;
    }
    const appended = yield* append(address, items, { expectedOffset: offset }).pipe(
      Effect.catchTag("OffsetMismatch", (mismatch) => Effect.fail(new LostRace(mismatch))),
    );
    const decided: Decided<B> = { value, offset: appended.offset, appended: true };
    return decided;
  });
  return yield* retryLostRace(attempt, spec.retry ?? defaultDecideRetry);
});
/**
 * Marks the `OffsetMismatch` of a decide loop's own append. Only this error is
 * retried: an `OffsetMismatch` that the caller's `decide` raises is its own
 * failure and passes through.
 */
class LostRace {
  constructor(readonly mismatch: OffsetMismatch) {}
}
/**
 * Retry `attempt` after a `LostRace` while `schedule` allows, then fail with the
 * last `OffsetMismatch`. `retry` steps its schedule with every error it sees, so
 * every other outcome passes the retry as a `Result` and is raised after.
 */
const retryLostRace = Effect.fnUntraced(function* <A, E, R>(
  attempt: Effect.Effect<A, E, R>,
  schedule: Schedule.Schedule<unknown, OffsetMismatch>,
) {
  const outcome = yield* attempt.pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        error instanceof LostRace
          ? Effect.fail(error.mismatch)
          : // SAFETY: `instanceof` ruled out `LostRace`; TS cannot narrow a type parameter.
            Effect.succeed(Result.fail(error as Exclude<E, LostRace>)),
      onSuccess: (value) => Effect.succeed(Result.succeed(value)),
    }),
    Effect.retry(schedule),
  );
  return yield* Effect.fromResult(outcome);
});
export interface FollowOptions extends ReadOptions {
  /**
   * Schedule for retrying a failed read after a retryable `TransportFault` or `StorageFault`.
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

interface FollowState {
  readonly offset: string | undefined;
  readonly live: boolean;
  /** Last `Stream-Cursor` from a live read; echoed on the next one for CDN collapsing. */
  readonly cursor: string | undefined;
}
/**
 * Catch up, then wait one readNext at a time, filtering empty open batches.
 * A read that fails with a retryable transport or storage fault is retried from the same
 * offset, so a restart or network drop yields each item once.
 */
export function follow<A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
  options: FollowOptions = {},
): Stream.Stream<Batch<A>, FollowFailure, StreamsReader | RD> {
  const { retry = defaultFollowRetry, ...readOptions } = options;
  const retrying = <T, E extends { readonly _tag: string }, R>(read: Effect.Effect<T, E, R>) =>
    read.pipe(
      Effect.tapError((error) =>
        isRetryableFault(error) ? Effect.logWarning("Streams.follow retry", error) : Effect.void,
      ),
      Effect.retry({ while: isRetryableFault, schedule: retry }),
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
): Effect.fn.Return<HeadResult, HeadFailure, StreamsReader> {
  return yield* (yield* StreamsReader).head(address.id);
});
export const remove = Effect.fn("Streams.remove")(function* <A, RD, RE>(
  address: StreamAddress<A, RD, RE>,
): Effect.fn.Return<void, RemoveFailure, StreamsWriter> {
  return yield* (yield* StreamsWriter).remove(address.id);
});
