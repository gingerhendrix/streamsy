import { Protocol } from "@streamsy/core/protocol";
import * as Fetch from "@streamsy/core/fetch";
import { Effect, Schema, type Scope } from "effect";
import { Streamsy, StreamAddress } from "@streamsy/core";
import { Streams, type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Output,
  Projection,
  Transaction,
  type ProjectionFault,
} from "@streamsy/projection";
class Input extends Streamsy.json<Input>()("ProjectionTypes/Input", "types/input/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}
class Other extends Streamsy.json<Other>()("ProjectionTypes/Other", "types/other/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}
class Result extends Streamsy.bytes<Result>()("ProjectionTypes/Result", "types/result/:id", {
  params: { id: Schema.String },
}) {}
const atomic = Projection.atomic({
  id: "types/atomic",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  process: () => Effect.void,
}).at({});
const declared = Projection.outputs({
  id: "types/named",
  params: { id: Schema.String },
  inputs: { input: Input, other: Other },
  outputs: { result: Output.stream(Schema.Finite, { stream: Result }) },
  process: (batch) => Effect.succeed({ result: batch.input.items }),
});
const named = declared.at({ id: "a" });
const atomicRun = Projection.run(atomic);
const namedRun = Projection.run(named);
const namedWatch = Projection.watch(named);
const namedFollow = Projection.follow(named);
const Totals = Schema.Struct({ sum: Schema.Finite, words: Schema.Array(Schema.String) });
const folded = Projection.atomic({
  id: "types/folded",
  params: { id: Schema.String },
  inputs: { input: Input, other: Other },
  state: { schema: Totals, initial: { sum: 0, words: [] } },
  fold: (totals, entry) =>
    entry.input === "input"
      ? { ...totals, sum: totals.sum + entry.item }
      : { ...totals, words: [...totals.words, entry.item] },
});
const foldedRead = Projection.read(folded.at({ id: "a" }));
const statefulOutputs = Projection.outputs({
  id: "types/stateful-outputs",
  params: { id: Schema.String },
  inputs: { input: Input },
  outputs: { result: Output.stream(Schema.Finite, { stream: Result }) },
  state: { schema: Schema.Finite, initial: ({ id }) => id.length },
  process: (count, batch) =>
    Effect.succeed({ state: count + batch.input.items.length, result: batch.input.items }),
});
const statefulRead = Projection.read(statefulOutputs.at({ id: "a" }));
const statelessRead = Projection.read(named);
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;
type SuccessOf<T> = T extends Effect.Effect<infer A, any, any> ? A : never;
export type Checks = [
  Assert<Same<SuccessOf<typeof foldedRead>["state"], typeof Totals.Type>>,
  Assert<Same<keyof SuccessOf<typeof foldedRead>["offsets"], "input" | "other">>,
  Assert<Same<SuccessOf<typeof foldedRead>["offsets"]["other"], string>>,
  Assert<Same<SuccessOf<typeof statefulRead>["state"], number>>,
  Assert<Same<keyof SuccessOf<typeof statefulRead>["offsets"], "input">>,
  Assert<Same<SuccessOf<typeof statelessRead>["state"], undefined>>,
  Assert<Same<keyof SuccessOf<typeof statelessRead>["offsets"], "input" | "other">>,
  Assert<Same<keyof SuccessOf<typeof namedRun>["offsets"], "input" | "other">>,
  Assert<Same<Effect.Services<typeof foldedRead>, CheckpointStore>>,
  Assert<Same<typeof statefulOutputs.state.id, string>>,
  Assert<Same<Effect.Services<typeof atomicRun>, Input | Transaction | CheckpointStore>>,
  Assert<Same<Effect.Services<typeof namedRun>, Input | Other | Result | CheckpointStore>>,
  Assert<
    Same<
      Effect.Services<typeof namedWatch>,
      Input | Other | Streamsy.ChangeFeed<Input | Other> | Result | CheckpointStore | Scope.Scope
    >
  >,
  Assert<
    Same<
      Effect.Services<typeof namedFollow>,
      Input | Other | Result | CheckpointStore | Scope.Scope
    >
  >,
  Assert<Same<Extract<Effect.Services<typeof namedRun>, StreamsReader | StreamsWriter>, never>>,
];
const missing = namedRun.pipe(
  Effect.provide(Protocol.bind(Streams.layerMemory(), Streamsy.group(Input, Other))),
);
// oxlint-disable effecttsgo/missing-effect-context -- This negative test deliberately omits Result.
// @ts-expect-error the declared output stream must be provided.
const incomplete: Effect.Effect<unknown, ProjectionFault, CheckpointStore | Transaction> = missing;
void incomplete;
// oxlint-enable effecttsgo/missing-effect-context
Projection.atomic({
  id: "bare",
  params: {},
  // @ts-expect-error plain refs do not carry a stream key
  inputs: { input: StreamAddress.json("plain", { schema: Schema.Finite }) },
  process: () => Effect.void,
}).at({});

// Widening loses the exact keys, but must not erase the unresolved requirement.
// oxlint-disable effecttsgo/any-unknown-in-error-context, effecttsgo/missing-effect-context -- These negative tests deliberately widen declarations.
const widenedInputs: import("@streamsy/projection").InputMap = { input: Input.at({ id: "a" }) };
const widened = Projection.run(
  Projection.atomic({
    id: "widened",
    params: {},
    inputs: widenedInputs,
    process: () => Effect.void,
  }).at({}),
);
const widenedOutput: Output.Stream<number> = Output.stream(Schema.Finite, {
  stream: Result.at({ id: "a" }),
});
const widenedNamed = Projection.run(
  Projection.outputs({
    id: "widened-output",
    params: {},
    inputs: { input: Input.at({ id: "a" }) },
    outputs: { result: widenedOutput },
    process: () => Effect.succeed({ result: [1] }),
  }).at({}),
);
export type WidenedChecks = [
  Assert<Same<Effect.Services<typeof widened>, unknown>>,
  Assert<Same<Effect.Services<typeof widenedNamed>, unknown>>,
];
// @ts-expect-error a widened input map retains an unsatisfied requirement
const cannotRunWidened: Effect.Effect<unknown, ProjectionFault> = widened;
// @ts-expect-error a widened output declaration retains an unsatisfied requirement
const cannotRunWidenedOutput: Effect.Effect<unknown, ProjectionFault> = widenedNamed;
void cannotRunWidened;
void cannotRunWidenedOutput;
// oxlint-enable effecttsgo/any-unknown-in-error-context, effecttsgo/missing-effect-context

// oxlint-disable effecttsgo/missing-effect-context -- Negative tests deliberately omit Streamsy.ChangeFeed<Input>.
// The protocol-only graph deliberately hides Storage and cannot satisfy a feed requirement.
const protocolOnly: import("effect").Layer.Layer<StreamsReader | StreamsWriter> =
  Streams.layerMemory();
const withoutFeed = Projection.watch(atomic).pipe(
  Effect.provide(Protocol.bind(protocolOnly, Streamsy.group(Input))),
);
// @ts-expect-error watch requires Streamsy.ChangeFeed<Input> on a host without a feed.
const missingFeed: Effect.Effect<unknown, ProjectionFault, CheckpointStore | Scope.Scope> =
  withoutFeed;
void missingFeed;

const fetchWatch = Projection.watch(atomic).pipe(
  Effect.provide(Fetch.layer({ baseUrl: "https://example.test" }, Streamsy.group(Input))),
);
// @ts-expect-error Fetch supplies the stream, but cannot supply its local Streamsy.ChangeFeed capability.
const missingFetchFeed: Effect.Effect<
  unknown,
  ProjectionFault | import("@streamsy/core").TransportFault,
  CheckpointStore | Scope.Scope | import("effect/unstable/http/HttpClient").HttpClient
> = fetchWatch;
void missingFetchFeed;

// oxlint-enable effecttsgo/missing-effect-context

export type FeedChecks = [
  Assert<
    Same<
      Effect.Services<typeof withoutFeed>,
      Streamsy.ChangeFeed<Input> | Transaction | CheckpointStore | Scope.Scope
    >
  >,
  Assert<
    Same<
      Effect.Services<typeof fetchWatch>,
      | Streamsy.ChangeFeed<Input>
      | Transaction
      | CheckpointStore
      | Scope.Scope
      | import("effect/unstable/http/HttpClient").HttpClient
    >
  >,
];

Projection.atomic({
  id: "single-input-removed",
  params: {},
  // @ts-expect-error A projection requires an inputs map.
  input: Input.at({ id: "a" }),
  process: () => Effect.void,
}).at({});
Projection.outputs({
  id: "single-outputs-input-removed",
  params: {},
  // @ts-expect-error An outputs projection also requires an inputs map.
  input: Input.at({ id: "a" }),
  outputs: { output: Output.stream(Schema.String, { stream: Other.at({ id: "a" }) }) },
  process: () => Effect.succeed({ output: [] }),
}).at({});

// @ts-expect-error every projection declares its params, even when there are none.
Projection.atomic({
  id: "no-params",
  inputs: { input: Input.at({ id: "a" }) },
  process: () => Effect.void,
});
// @ts-expect-error at requires the declared params.
declared.at({});
// @ts-expect-error at checks the decoded param types.
declared.at({ id: 1 });
// oxlint-disable effecttsgo/any-unknown-in-error-context, effecttsgo/floating-effect -- This negative test deliberately passes an unbound declaration.
// @ts-expect-error runners take the bound value, not the declaration.
Projection.run(declared);
// oxlint-enable effecttsgo/any-unknown-in-error-context, effecttsgo/floating-effect

// @ts-expect-error fold needs a state: field
Projection.atomic({
  id: "fold-without-state",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  fold: (sum: number) => sum,
});
// @ts-expect-error an atomic projection with state: takes fold, not process
Projection.atomic({
  id: "state-without-fold",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  state: { schema: Schema.Finite, initial: 0 },
  process: () => Effect.void,
});
Projection.atomic({
  id: "fold-returns-wrong-state",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  state: { schema: Schema.Finite, initial: 0 },
  // @ts-expect-error the step returns the declared state type
  fold: () => "wrong",
});
Projection.outputs({
  id: "state-arg-without-state",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  outputs: {},
  // @ts-expect-error without state: the handler gets no state argument
  process: (_state: number, _batch: string) => Effect.succeed({}),
});
// @ts-expect-error a stateless declaration has no state source
void declared.state;
