/** Compile-time contract for `Projection.decide`. */
import { Effect, Schema } from "effect";
import {
  Streamsy,
  type AppendBaseError,
  type Decided,
  type EncodeFault,
  type OffsetMismatch,
  type StreamsFault,
} from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  Projection,
  type ProjectionFault,
  type Snapshot,
} from "@streamsy/projection";

type Equal<A, B> =
  // oxlint-disable-next-line typescript/no-unnecessary-type-parameters -- SAFETY: quantified conditional functions are required for this exact type-equality proof.
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

class Numbers extends Streamsy.json<Numbers>()("decideTypes/Numbers", "numbers", {
  params: {},
  schema: Schema.Finite,
}) {}

class Words extends Streamsy.json<Words>()("decideTypes/Words", "words", {
  params: {},
  schema: Schema.String,
}) {}

const totals = Projection.fold({
  id: "decide-types",
  params: {},
  inputs: { numbers: Numbers, words: Words },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (sum, entry) => (entry.input === "numbers" ? sum + entry.item : sum),
}).at({});

// The snapshot carries the state and both offsets; the result and errors are exact.
const decided = Projection.decide(totals, {
  input: "words",
  decide: ({ state, offsets }) =>
    Effect.succeed({ value: `${state}@${offsets.numbers}`, items: [offsets.words] }),
});

export const decidedType: Equal<
  typeof decided,
  Effect.Effect<
    Decided<string>,
    OffsetMismatch | AppendBaseError | ProjectionFault | StreamsFault | EncodeFault,
    Numbers | Words | CheckpointStore
  >
> = true;

export const wrong = [
  Projection.decide(totals, {
    // @ts-expect-error `input` must name one of the projection's inputs
    input: "letters",
    decide: () => Effect.succeed({ value: 0, items: [] }),
  }),
  Projection.decide(totals, {
    input: "numbers",
    // @ts-expect-error the items are typed by the named input: numbers, not strings
    decide: () => Effect.succeed({ value: 0, items: ["one"] }),
  }),
  Projection.decide(totals, {
    input: "words",
    // @ts-expect-error the items are typed by the named input: strings, not numbers
    decide: () => Effect.succeed({ value: 0, items: [1] }),
  }),
];

const stateless = Projection.process({
  id: "stateless-decide",
  params: {},
  inputs: { numbers: Numbers },
  commit: Commit.atLeastOnce,
  process: () => Effect.void,
}).at({});

// @ts-expect-error decide requires declared value state
export const noStateDecision = Projection.decide(stateless, {
  input: "numbers",
  decide: () => Effect.succeed({ value: 0, items: [] }),
});

// @ts-expect-error current requires declared value state
export const noStateCurrent = Projection.current(stateless);

const accumulating = Projection.mapAccum({
  id: "current-output-types",
  params: {},
  inputs: { numbers: Numbers },
  outputs: { words: Words },
  state: { schema: Schema.Finite, initial: 0 },
  commit: Commit.producer,
  mapAccum: (sum, { item }) => [sum + item, { words: [String(item)] }] as const,
}).at({});

const currentAccumulating = Projection.current(accumulating);

// Reading accumulated state requires the input and checkpoint services, not output services.
export const currentAccumulatingType: Equal<
  typeof currentAccumulating,
  Effect.Effect<
    Snapshot<number, typeof accumulating.inputs>,
    ProjectionFault,
    Numbers | CheckpointStore
  >
> = true;
