import { Effect, Option, Predicate, Schema } from "effect";
import {
  Offset,
  StreamsReader,
  type ReadMessage,
  type ReadResult,
  type StreamRef,
} from "@streamsy/core";
import { emptySlice, type InputMap, type Slice, type Slices } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import type { PinnedRange } from "./unit.ts";

export interface Budget {
  /** Passes per run. */
  readonly units?: number;
  /** Items per pass across every input. */
  readonly items?: number;
  /** Payload bytes per pass across every input. */
  readonly bytes?: number;
}
export const DEFAULT_UNITS = 100;
export const DEFAULT_ITEMS = 1000;

export const validateBudget = (budget: Budget) =>
  Effect.suspend(() => {
    const values = [
      budget.units ?? DEFAULT_UNITS,
      budget.items ?? DEFAULT_ITEMS,
      ...(budget.bytes === undefined ? [] : [budget.bytes]),
    ];
    return values.every((value) => Number.isSafeInteger(value) && value > 0)
      ? Effect.void
      : Effect.fail(
          new ProjectionFault({
            phase: "load",
            reason: "invalid-budget",
            message: "Budget values must be positive safe integers",
          }),
        );
  });

export interface Read<A> {
  readonly slice: Slice<A>;
  readonly bytes: number;
}

/** One bounded read after `from`; retain input history and never reuse a deleted stream id. */
export const readSlice = Effect.fn("Projection.readSlice")(function* <A>(
  input: string,
  ref: StreamRef.StreamRef<A>,
  from: string,
  limit: number,
): Effect.fn.Return<Read<A>, ProjectionFault, StreamsReader> {
  const reader = yield* StreamsReader;
  const historyUnavailable = () =>
    new ProjectionFault({
      phase: "read",
      reason: "history-unavailable",
      input,
      message: `Required history of ${ref.id} after ${from} is unavailable`,
    });
  const storageFailure = () =>
    new ProjectionFault({
      phase: "read",
      reason: "storage-failure",
      input,
      message: `Cannot read ${ref.id}`,
    });
  if (Option.isNone(Schema.decodeOption(Offset)(from)))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "invalid-record",
      input,
      message: `Stored offset for ${input} is not a valid offset`,
    });
  const result = yield* reader.read(ref.id, { offset: from, limit }).pipe(
    Effect.catchTags({
      StreamNotFound: () => historyUnavailable(),
      StreamGone: () => historyUnavailable(),
      StorageFault: () => storageFailure(),
      TransportFault: () => storageFailure(),
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
    bytes: byteLength(result.messages),
  };
});

const byteLength = (messages: ReadonlyArray<ReadMessage>): number =>
  messages.reduce((sum, message) => sum + message.data.byteLength, 0);

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

/**
 * Reproduces a pinned range exactly: `count` messages after `from`, ending at
 * `nextOffset`. Pages are looped because a backend may answer short and the HTTP
 * edge caps a page. Byte budgets do not apply; the range was already accepted.
 */
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
  const messages: Array<ReadMessage> = [];
  let cursor = range.from;
  let last: ReadResult | undefined;
  while (messages.length < range.count) {
    const page = yield* reader
      .read(ref.id, { offset: cursor, limit: range.count - messages.length })
      .pipe(
        Effect.catchTags({
          StreamNotFound: () => unreproducible("the input is missing"),
          StreamGone: () => unreproducible("the input is gone"),
          StorageFault: () =>
            new ProjectionFault({
              phase: "pin",
              reason: "storage-failure",
              input,
              message: `Cannot read ${ref.id}`,
            }),
          TransportFault: () =>
            new ProjectionFault({
              phase: "pin",
              reason: "storage-failure",
              input,
              message: `Cannot read ${ref.id}`,
            }),
        }),
      );
    if (page.messages.length === 0) return yield* unreproducible("the input ended early");
    if (page.messages.length > range.count - messages.length)
      return yield* unreproducible("the input answered past the pinned count");
    messages.push(...page.messages);
    cursor = page.nextOffset;
    last = page;
  }
  if (last === undefined || cursor !== range.nextOffset)
    return yield* unreproducible(`the range ends at ${cursor}, not ${range.nextOffset}`);
  return {
    slice: {
      from: range.from,
      items: yield* decodeMessages(input, ref, messages),
      nextOffset: range.nextOffset,
      upToDate: last.upToDate,
      closed: last.closed,
    },
    bytes: byteLength(messages),
  };
});

export interface Pass<Inputs extends InputMap> {
  readonly slices: Slices<Inputs>;
  readonly items: number;
  readonly bytes: number;
  /** The pass was refused: a slice exceeded the byte budget before any input contributed. */
  readonly refused: boolean;
}

/**
 * Reads every input in declaration order with the item budget carried forward.
 * A slice that exceeds the remaining bytes before any input contributed refuses
 * the whole pass: later inputs are not read and the pass reports `limit-reached`,
 * so declaration order keeps its priority. Once an earlier input contributed, an
 * oversized later slice is skipped and the pass still commits that progress.
 */
export const readInputs = Effect.fn("Projection.readInputs")(function* <Inputs extends InputMap>(
  inputs: Inputs,
  offsets: Record<string, string>,
  budget: { readonly items: number; readonly bytes?: number },
): Effect.fn.Return<Pass<Inputs>, ProjectionFault, StreamsReader> {
  const slices: Record<string, Slice<unknown>> = {};
  let items = 0;
  let bytes = 0;
  let refused = false;
  for (const [name, ref] of Object.entries(inputs)) {
    const from = offsets[name] ?? "";
    const remaining = budget.items - items;
    if (refused || remaining <= 0) {
      slices[name] = emptySlice(from);
      continue;
    }
    const read = yield* readSlice(name, ref, from, remaining);
    if (budget.bytes !== undefined && read.bytes > budget.bytes - bytes) {
      slices[name] = emptySlice(from);
      if (items === 0) refused = true;
      continue;
    }
    slices[name] = read.slice;
    items += read.slice.items.length;
    bytes += read.bytes;
  }
  // SAFETY: `slices` has exactly the keys of `inputs`, each read through that input's codec.
  const typed = slices as Slices<Inputs>;
  return { slices: typed, items, bytes, refused };
});

/** Tagged items in declaration order, then stream order within each input. */
export const entries = (slices: Record<string, Slice<unknown>>) => {
  const out: Array<{ readonly input: string; readonly item: unknown; readonly index: number }> = [];
  for (const [input, slice] of Object.entries(slices)) {
    for (const item of slice.items) out.push({ input, item, index: out.length });
  }
  return out;
};
