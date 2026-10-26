import { Effect, Option, Predicate, Record, Schema } from "effect";
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
  emptySlice,
  type Entry,
  type InputServices,
  type InputMap,
  type Slice,
  type Slices,
} from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import { terminalOf, type PinnedRange } from "./unit.ts";

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

/**
 * A lost history is terminal only when a known input's head confirms it is gone.
 * A pinned unit is replayed with `reproduce` and never takes this path: a
 * removal between a pin and its append is a `range-unreproducible` fault.
 */
export const readOrGone = Effect.fn("Projection.readOrGone")(function* <A, R>(
  input: string,
  address: Streamsy.Address<A, R>,
  from: string,
  read: Effect.Effect<Read<A>, ProjectionFault, R>,
  seen: boolean,
): Effect.fn.Return<Read<A>, ProjectionFault, R> {
  return yield* read.pipe(
    Effect.catchTag("ProjectionFault", (fault) => {
      if (!seen || fault.reason !== "history-unavailable") return Effect.fail(fault);

      return Effect.gen(function* () {
        const { wire } = yield* address.stream;

        const gone = yield* wire.head(address.id).pipe(
          Effect.as(false),
          Effect.catchTags({
            StreamNotFound: () => Effect.succeed(true),
            StreamGone: () => Effect.succeed(true),
          }),
          Effect.mapError(
            (cause) =>
              new ProjectionFault({
                phase: "read",
                reason: "storage-failure",
                input,
                message: `Cannot read the head of ${address.id}`,
                cause,
              }),
          ),
        );

        if (!gone) return yield* fault;

        return { slice: { ...emptySlice<A>(from), upToDate: true, closed: true, gone: true } };
      });
    }),
  );
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

  if (range.count === 0) {
    if (range.from !== range.nextOffset || (!range.closed && !range.gone))
      return yield* unreproducible(
        "an empty pin must describe a terminal outcome at the same offset",
      );

    const slice: Slice<A> = {
      ...emptySlice<A>(range.from),
      nextOffset: range.nextOffset,
      closed: range.closed === true,
      upToDate: true,
      ...terminalOf(range),
    };

    return { slice };
  }

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
      upToDate: range.upToDate ?? (last.upToDate && cursor === range.nextOffset),
      closed: range.closed === true,
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
  seen = false,
): Effect.fn.Return<Pass<Inputs>, ProjectionFault, InputServices<Inputs>> {
  const slices: Record<string, Slice<unknown>> = {};
  let items = 0;

  for (const [name, address] of inputEntries(inputs)) {
    const from = offsets[name] ?? "";
    const read = yield* readOrGone(name, address, from, readSlice(name, address, from), seen);
    slices[name] = read.slice;
    items += read.slice.items.length;
  }

  // SAFETY: `slices` has exactly the keys of `inputs`, each read through that input's codec.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: TypeScript cannot correlate Object.entries keys with each decoded input codec.
  return { slices: slices as Slices<Inputs>, items };
});

/** Tagged items in declaration order, then stream order within each input. */
export const items = <Inputs extends InputMap>(
  batch: Slices<Inputs>,
): ReadonlyArray<Entry<Inputs>> => {
  const out: Array<{ readonly input: string; readonly item: unknown; readonly index: number }> = [];

  for (const [input, slice] of Record.toEntries<string, Slice<unknown>>(batch)) {
    for (const item of slice.items) out.push({ input, item, index: out.length });
  }

  // SAFETY: each entry's item was decoded by the codec of the input it is tagged with.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: TypeScript cannot correlate the tagged input key with its already decoded item.
  return out as ReadonlyArray<Entry<Inputs>>;
};
