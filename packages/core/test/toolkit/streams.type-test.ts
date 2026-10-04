/** Compile-time contract for typed appends and folds. */
import { Effect, Schema } from "effect";
import * as StreamAddress from "../../src/toolkit/address.ts";
import * as Streams from "../../src/toolkit/streams.ts";
import { Streamsy } from "../../src/index.ts";
import type {
  AppendBaseError,
  AppendErrorFor,
  OffsetMismatch,
  ProducerGap,
  ReadError,
  StaleEpoch,
} from "../../src/protocol/errors.ts";
import type { StreamsReader, StreamsWriter } from "../../src/protocol/tags.ts";
import type { AppendCallOptions } from "../../src/protocol/options.ts";
import type { DecodeFault, EncodeFault, StreamsFault } from "../../src/fault.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type ErrorOf<T> = T extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;
type ProducerError = StaleEpoch | ProducerGap;
/** What every append can fail with. */
type Base = AppendBaseError | StreamsFault | EncodeFault;

const address = StreamAddress.json("events", { schema: Schema.Struct({ n: Schema.Finite }) });
const item = { n: 1 };
const producer = { producerId: "p", producerEpoch: 0, producerSeq: 0 };

// The error type follows the options.
const plain = Streams.append(address, [item]);
const conditional = Streams.append(address, [item], { expectedOffset: "0" });
const fenced = Streams.append(address, [item], { producer });
const both = Streams.append(address, [item], { producer, expectedOffset: "0" });
const closing = Streams.append(address, [], { close: true });
export const plainErrors: Equal<ErrorOf<typeof plain>, Base> = true;
export const conditionalErrors: Equal<ErrorOf<typeof conditional>, Base | OffsetMismatch> = true;
export const fencedErrors: Equal<ErrorOf<typeof fenced>, Base | ProducerError> = true;
export const bothErrors: Equal<ErrorOf<typeof both>, Base | OffsetMismatch | ProducerError> = true;
export const closingErrors: Equal<ErrorOf<typeof closing>, Base> = true;

// Options that may hold a precondition keep its rejection.
declare const maybe: AppendCallOptions;
const unknownOptions = Streams.append(address, [item], maybe);
export const maybeErrors: Equal<
  ErrorOf<typeof unknownOptions>,
  Base | OffsetMismatch | ProducerError
> = true;
export const forUnknown: Equal<
  AppendErrorFor<AppendCallOptions>,
  AppendBaseError | OffsetMismatch | ProducerError
> = true;
export const forNone: Equal<AppendErrorFor<{}>, AppendBaseError> = true;

// An append needs items unless it closes.
declare const items: ReadonlyArray<{ readonly n: number }>;
declare const maybeClose: boolean;
export const appends = [
  // @ts-expect-error an empty append always fails on the wire
  Streams.append(address, []),
  // @ts-expect-error a precondition does not make an empty append valid
  Streams.append(address, [], { expectedOffset: "0" }),
  // @ts-expect-error a plain array may be empty
  Streams.append(address, items),
  Streams.append(address, items, { close: true }),
  // @ts-expect-error a close that may be false still needs items
  Streams.append(address, [], { close: maybeClose }),
];

// The stream service has the same rules.
class Events extends Streamsy.json<Events>()("StreamsTypes/Events", "events", {
  params: {},
  schema: Schema.Struct({ n: Schema.Finite }),
}) {}
export const service = Effect.gen(function* () {
  const events = yield* Events;
  const appended = events.append({}, [item], { expectedOffset: "0" });
  const conditionalService: Equal<ErrorOf<typeof appended>, Base | OffsetMismatch> = true;
  // @ts-expect-error the service append needs items too
  const empty = events.append({}, []);
  const folded = yield* events.fold(
    {},
    () => 0,
    (sum, event) => sum + event.n,
  );
  const foldResult: Equal<typeof folded, Streams.Folded<number>> = true;
  const collected = yield* events.collect({});
  const collectResult: Equal<typeof collected, Streams.Collected<{ readonly n: number }>> = true;
  return [empty, conditionalService, foldResult, collectResult];
});

// A fold's reducer gets the address's item type.
const byLength = (sum: number, value: string) => sum + value.length;
// @ts-expect-error the items are `{ n: number }`, not strings
export const wrongItems = Streams.fold(address, () => 0, byLength);

// `decide` keeps its own error, the lost race, and every read and append rejection.
class Refused extends Schema.TaggedError<Refused>()("Refused", {}) {}
const decided = Streams.decide(address, {
  initial: () => 0,
  fold: (sum, event) => sum + event.n,
  decide: (sum, { offset }) =>
    sum > 10 ? Effect.fail(new Refused()) : Effect.succeed({ value: offset, items: [{ n: sum }] }),
});
type DecideError =
  | Refused
  | OffsetMismatch
  | AppendBaseError
  | ReadError
  | StreamsFault
  | EncodeFault
  | DecodeFault;
export const decideErrors: Equal<ErrorOf<typeof decided>, DecideError> = true;
export const decideResult: Equal<
  typeof decided,
  Effect.Effect<Streams.Decided<string>, DecideError, StreamsReader | StreamsWriter>
> = true;
export const wrongDecision = Streams.decide(address, {
  initial: () => 0,
  fold: (sum, event) => sum + event.n,
  // @ts-expect-error the items to append are `{ n: number }`
  decide: () => Effect.succeed({ value: 0, items: ["one"] }),
});

// The service `decide` has the same types and provides the wires.
export const serviceDecide = Effect.gen(function* () {
  const events = yield* Events;
  const result = events.decide(
    {},
    {
      initial: () => 0,
      fold: (sum, event) => sum + event.n,
      decide: (sum) => Effect.succeed({ value: sum, items: [] }),
    },
  );
  const serviceDecided: Equal<
    typeof result,
    Effect.Effect<
      Streams.Decided<number>,
      OffsetMismatch | AppendBaseError | ReadError | StreamsFault | EncodeFault | DecodeFault
    >
  > = true;
  return serviceDecided;
});
