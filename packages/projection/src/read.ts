import { Effect, Option, Predicate, Schema } from "effect";
import {
  Offset,
  compare,
  type ReadMessage,
  type ReadResult,
  type StreamAddress,
  type Streamsy,
} from "@streamsy/core";
import {
  inputEntries,
  type Entry,
  type InputServices,
  type InputMap,
  type Slice,
  type Slices,
} from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import type { PinnedRange } from "./unit.ts";

export interface RunOptions {
  /** Maximum checkpoint transactions per call; absent means run to completion. */
  readonly limit?: number;
}

export const validateOptions = (options: RunOptions) =>
  Effect.suspend(() =>
    options.limit === undefined || (Number.isSafeInteger(options.limit) && options.limit > 0)
      ? Effect.void
      : Effect.fail(
          new ProjectionFault({
            phase: "load",
            reason: "invalid-options",
            message: "limit must be a positive safe integer",
          }),
        ),
  );

export interface Read<A> {
  readonly slice: Slice<A>;
}

/** One bounded read after `from`; retain input history and never reuse a deleted stream id. */
export const readSlice = Effect.fn("Projection.readSlice")(function* <A, R>(
  input: string,
  address: Streamsy.Address<A, R>,
  from: string,
): Effect.fn.Return<Read<A>, ProjectionFault, R> {
  const { wire: reader } = yield* address.stream;

  const historyUnavailable = () =>
    new ProjectionFault({
      phase: "read",
      reason: "history-unavailable",
      input,
      message: `Required history of ${address.id} after ${from} is unavailable`,
    });

  const storageFailure = (cause: unknown) =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input,
      message: `Cannot read ${address.id}`,
      cause,
    });

  if (Option.isNone(Schema.decodeOption(Offset)(from)))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "invalid-record",
      input,
      message: `Stored offset for ${input} is not a valid offset`,
    });

  const result = yield* reader.read(address.id, { offset: from }).pipe(
    Effect.catchTags({
      InvalidReadRequest: (cause) =>
        new ProjectionFault({
          phase: "read",
          reason: "invalid-source",
          input,
          message: cause.message,
          cause,
        }),
      StreamNotFound: () => historyUnavailable(),
      StreamGone: () => historyUnavailable(),
      StorageFault: storageFailure,
      TransportFault: storageFailure,
    }),
  );

  if (result.nextOffset < from || (result.messages.length === 0 && result.nextOffset !== from))
    return yield* historyUnavailable();

  if (result.nextOffset === from && (result.messages.length > 0 || !result.upToDate))
    return yield* new ProjectionFault({
      phase: "read",
      reason: "invalid-source",
      input,
      message: `Read of ${address.id} did not advance past ${from}`,
    });
  const items = yield* decodeMessages(input, address, result.messages);

  return {
    slice: {
      from,
      items,
      nextOffset: result.nextOffset,
      upToDate: result.upToDate,
      closed: result.closed,
    },
  };
});

/** Decode failure is an input fault: the stored bytes do not match the declared codec. */
const decodeMessages = <A>(
  input: string,
  address: StreamAddress.StreamAddress<A>,
  messages: ReadonlyArray<ReadMessage>,
) =>
  Effect.forEach(messages, (message, index) =>
    Schema.decodeEffect(address.codec)(
      Predicate.isTagged(address, "Json") ? new TextDecoder().decode(message.data) : message.data,
    ).pipe(
      Effect.mapError(
        () =>
          new ProjectionFault({
            phase: "read",
            reason: "invalid-source",
            input,
            message: `Cannot decode ${address.id} at read message ${index}`,
          }),
      ),
    ),
  );

/** Re-read server pages, retaining the pinned count and checking the boundary where available. */
export const reproduce = Effect.fn("Projection.reproduce")(function* <A, R>(
  input: string,
  address: Streamsy.Address<A, R>,
  range: PinnedRange,
): Effect.fn.Return<Read<A>, ProjectionFault, R> {
  const { wire: reader } = yield* address.stream;

  const unreproducible = (detail: string) =>
    new ProjectionFault({
      phase: "pin",
      reason: "range-unreproducible",
      input,
      message: `Pinned range of ${address.id} after ${range.from} cannot be reproduced: ${detail}`,
    });

  let messages: Array<ReadMessage> = [];
  let cursor = range.from;
  let last: ReadResult | undefined;

  while (messages.length < range.count) {
    const page = yield* reader.read(address.id, { offset: cursor }).pipe(
      Effect.catchTags({
        InvalidReadRequest: (cause) =>
          new ProjectionFault({
            phase: "pin",
            reason: "invalid-record",
            input,
            message: cause.message,
            cause,
          }),
        StreamNotFound: () => unreproducible("the input is missing"),
        StreamGone: () => unreproducible("the input is gone"),
        StorageFault: (cause) =>
          new ProjectionFault({
            phase: "pin",
            reason: "storage-failure",
            input,
            message: `Cannot read ${address.id}`,
            cause,
          }),
        TransportFault: (cause) =>
          new ProjectionFault({
            phase: "pin",
            reason: "storage-failure",
            input,
            message: `Cannot read ${address.id}`,
            cause,
          }),
      }),
    );

    if (page.messages.length === 0) return yield* unreproducible("the input ended early");
    messages.push(...page.messages);
    cursor = page.nextOffset;
    last = page;

    if (page.upToDate && messages.length < range.count)
      return yield* unreproducible("the input ended early");
  }

  if (last === undefined) return yield* unreproducible("the input ended early");

  if (messages.length === range.count) {
    if (cursor !== range.nextOffset)
      return yield* unreproducible(`the range ends at ${cursor}, not ${range.nextOffset}`);
  } else {
    messages = messages.slice(0, range.count);

    if (compare(cursor, range.nextOffset) < 0)
      return yield* unreproducible("the input is shorter than the pin");
  }

  return {
    slice: {
      from: range.from,
      items: yield* decodeMessages(input, address, messages),
      nextOffset: range.nextOffset,
      upToDate: last.upToDate && cursor === range.nextOffset,
      closed: last.closed && cursor === range.nextOffset,
    },
  };
});

export interface Pass<Inputs extends InputMap> {
  readonly slices: Slices<Inputs>;
  readonly items: number;
}

/** Read one server page per input in declaration order. */
export const readInputs = Effect.fn("Projection.readInputs")(function* <Inputs extends InputMap>(
  inputs: Inputs,
  offsets: Record<string, string>,
): Effect.fn.Return<Pass<Inputs>, ProjectionFault, InputServices<Inputs>> {
  const slices: Record<string, Slice<unknown>> = {};
  let items = 0;

  for (const [name, address] of inputEntries(inputs)) {
    const read = yield* readSlice(name, address, offsets[name] ?? "");
    slices[name] = read.slice;
    items += read.slice.items.length;
  }

  // SAFETY: `slices` has exactly the keys of `inputs`, each read through that input's codec.
  return { slices: slices as Slices<Inputs>, items };
});

/** Tagged items in declaration order, then stream order within each input. */
export const items = <Inputs extends InputMap>(
  batch: Slices<Inputs>,
): ReadonlyArray<Entry<Inputs>> => {
  const out: Array<{ readonly input: string; readonly item: unknown; readonly index: number }> = [];

  for (const [input, slice] of Object.entries<Slice<unknown>>(batch)) {
    for (const item of slice.items) out.push({ input, item, index: out.length });
  }

  // SAFETY: each entry's item was decoded by the codec of the input it is tagged with.
  return out as ReadonlyArray<Entry<Inputs>>;
};
