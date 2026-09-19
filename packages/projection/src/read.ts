import { Effect, Option, Predicate, Schema } from "effect";
import {
  Offset,
  compare,
  StreamsReader,
  type ReadMessage,
  type ReadResult,
  type StreamRef,
} from "@streamsy/core";
import { type InputMap, type Slice, type Slices } from "./batch.ts";
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
export const readSlice = Effect.fn("Projection.readSlice")(function* <A>(
  input: string,
  ref: StreamRef.StreamRef<A>,
  from: string,
): Effect.fn.Return<Read<A>, ProjectionFault, StreamsReader> {
  const reader = yield* StreamsReader;
  const historyUnavailable = () =>
    new ProjectionFault({
      phase: "read",
      reason: "history-unavailable",
      input,
      message: `Required history of ${ref.id} after ${from} is unavailable`,
    });
  const storageFailure = (cause: unknown) =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input,
      message: `Cannot read ${ref.id}`,
      cause,
    });
  if (Option.isNone(Schema.decodeOption(Offset)(from)))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "invalid-record",
      input,
      message: `Stored offset for ${input} is not a valid offset`,
    });
  const result = yield* reader.read(ref.id, { offset: from }).pipe(
    Effect.catchTags({
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
      message: `Read of ${ref.id} did not advance past ${from}`,
    });
  const items = yield* decodeMessages(input, ref, result.messages);
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
  ref: StreamRef.StreamRef<A>,
  messages: ReadonlyArray<ReadMessage>,
) =>
  Effect.forEach(messages, (message, index) =>
    Schema.decodeEffect(ref.codec)(
      Predicate.isTagged(ref, "Json") ? new TextDecoder().decode(message.data) : message.data,
    ).pipe(
      Effect.mapError(
        () =>
          new ProjectionFault({
            phase: "read",
            reason: "invalid-source",
            input,
            message: `Cannot decode ${ref.id} at read message ${index}`,
          }),
      ),
    ),
  );

/** Re-read server pages, retaining the pinned count and checking the boundary where available. */
export const reproduce = Effect.fn("Projection.reproduce")(function* <A>(
  input: string,
  ref: StreamRef.StreamRef<A>,
  range: PinnedRange,
): Effect.fn.Return<Read<A>, ProjectionFault, StreamsReader> {
  const reader = yield* StreamsReader;
  const unreproducible = (detail: string) =>
    new ProjectionFault({
      phase: "pin",
      reason: "range-unreproducible",
      input,
      message: `Pinned range of ${ref.id} after ${range.from} cannot be reproduced: ${detail}`,
    });
  let messages: Array<ReadMessage> = [];
  let cursor = range.from;
  let last: ReadResult | undefined;
  while (messages.length < range.count) {
    const page = yield* reader.read(ref.id, { offset: cursor }).pipe(
      Effect.catchTags({
        StreamNotFound: () => unreproducible("the input is missing"),
        StreamGone: () => unreproducible("the input is gone"),
        StorageFault: (cause) =>
          new ProjectionFault({
            phase: "pin",
            reason: "storage-failure",
            input,
            message: `Cannot read ${ref.id}`,
            cause,
          }),
        TransportFault: (cause) =>
          new ProjectionFault({
            phase: "pin",
            reason: "storage-failure",
            input,
            message: `Cannot read ${ref.id}`,
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
      items: yield* decodeMessages(input, ref, messages),
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
): Effect.fn.Return<Pass<Inputs>, ProjectionFault, StreamsReader> {
  const slices: Record<string, Slice<unknown>> = {};
  let items = 0;
  for (const [name, ref] of Object.entries(inputs)) {
    const read = yield* readSlice(name, ref, offsets[name] ?? "");
    slices[name] = read.slice;
    items += read.slice.items.length;
  }
  // SAFETY: `slices` has exactly the keys of `inputs`, each read through that input's codec.
  return { slices: slices as Slices<Inputs>, items };
});

/** Tagged items in declaration order, then stream order within each input. */
export const entries = (slices: Record<string, Slice<unknown>>) => {
  const out: Array<{ readonly input: string; readonly item: unknown; readonly index: number }> = [];
  for (const [input, slice] of Object.entries(slices)) {
    for (const item of slice.items) out.push({ input, item, index: out.length });
  }
  return out;
};
