import { Commit } from "@streamsy/projection";
import { Protocol } from "@streamsy/core/protocol";
import { RemoteStreams } from "@streamsy/core/remote";
import { Effect, Schema, type Scope } from "effect";
import { Streamsy, StreamAddress, MemoryStorage } from "@streamsy/core";
import { type StreamsReader, type StreamsWriter } from "@streamsy/core/protocol";
import {
  CheckpointStore,
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
class Result extends Streamsy.json<Result>()("ProjectionTypes/Result", "types/result/:id", {
  schema: Schema.Finite,
  params: { id: Schema.String },
}) {}
const transactional = Projection.process({
  commit: Commit.transaction,
  id: "types/transactional",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  process: () => Effect.void,
}).at({});
const declared = Projection.emit({
  commit: Commit.producer,
  id: "types/named",
  params: { id: Schema.String },
  inputs: { input: Input, other: Other },
  outputs: { result: Result },
  emit: (entry) => Effect.succeed({ result: entry.input === "input" ? [entry.item] : [] }),
});
const named = declared.at({ id: "a" });
const processRun = Projection.run(transactional);
const namedRun = Projection.run(named);
const namedWatch = Projection.watch(named);
const namedFollow = Projection.follow(named);
const Totals = Schema.Struct({ sum: Schema.Finite, words: Schema.Array(Schema.String) });
const folded = Projection.fold({
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
const statefulOutputs = Projection.mapAccumBatch({
  commit: Commit.producer,
  id: "types/stateful-outputs",
  params: { id: Schema.String },
  inputs: { input: Input },
  outputs: { result: Result },
  state: { schema: Schema.Finite, initial: ({ id }) => id.length },
  mapAccumBatch: (count, batch) =>
    Effect.succeed([count + batch.input.items.length, { result: batch.input.items }] as const),
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
  Assert<Same<Extract<keyof typeof statefulOutputs, "state">, never>>,
  Assert<Same<Effect.Services<typeof processRun>, Input | Transaction | CheckpointStore>>,
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
  Effect.provide(Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Input, Other))),
);
// oxlint-disable effecttsgo/missing-effect-context -- This negative test deliberately omits Result.
// @ts-expect-error the declared output stream must be provided.
const incomplete: Effect.Effect<unknown, ProjectionFault, CheckpointStore | Transaction> = missing;
void incomplete;
// oxlint-enable effecttsgo/missing-effect-context
Projection.process({
  commit: Commit.transaction,
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
  Projection.process({
    commit: Commit.transaction,
    id: "widened",
    params: {},
    inputs: widenedInputs,
    process: () => Effect.void,
  }).at({}),
);
const widenedOutput: Streamsy.Address<number, unknown> = Result.at({ id: "a" });
const widenedNamed = Projection.run(
  Projection.emit({
    commit: Commit.producer,
    id: "widened-output",
    params: {},
    inputs: { input: Input.at({ id: "a" }) },
    outputs: { result: widenedOutput },
    emit: () => Effect.succeed({ result: [1] }),
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
  MemoryStorage.layerProtocol();
const withoutFeed = Projection.watch(transactional).pipe(
  Effect.provide(Protocol.bind(protocolOnly, Streamsy.group(Input))),
);
// @ts-expect-error watch requires Streamsy.ChangeFeed<Input> on a host without a feed.
const missingFeed: Effect.Effect<unknown, ProjectionFault, CheckpointStore | Scope.Scope> =
  withoutFeed;
void missingFeed;

const fetchWatch = Projection.watch(transactional).pipe(
  Effect.provide(RemoteStreams.layer(Streamsy.group(Input), { baseUrl: "https://example.test" })),
);
// @ts-expect-error RemoteStreams supplies the stream, but cannot supply its local Streamsy.ChangeFeed capability.
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

Projection.process({
  commit: Commit.transaction,
  id: "single-input-removed",
  params: {},
  // @ts-expect-error A projection requires an inputs map.
  input: Input.at({ id: "a" }),
  process: () => Effect.void,
}).at({});
Projection.emit({
  commit: Commit.producer,
  id: "single-outputs-input-removed",
  params: {},
  // @ts-expect-error An outputs projection also requires an inputs map.
  input: Input.at({ id: "a" }),
  outputs: { output: Other.at({ id: "a" }) },
  emit: () => Effect.succeed({ output: [] }),
}).at({});

// @ts-expect-error every projection declares its params, even when there are none.
Projection.process({
  commit: Commit.transaction,
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
Projection.fold({
  id: "fold-without-state",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  fold: () => 0,
});
Projection.fold({
  id: "state-without-fold",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  state: { schema: Schema.Finite, initial: 0 },
  // @ts-expect-error fold takes fold, not process
  process: () => Effect.void,
});
Projection.fold({
  id: "fold-returns-wrong-state",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  state: { schema: Schema.Finite, initial: 0 },
  // @ts-expect-error the step returns the declared state type
  fold: () => "wrong",
});
Projection.emit({
  commit: Commit.producer,
  id: "state-arg-without-state",
  params: {},
  inputs: { input: Input.at({ id: "a" }) },
  outputs: {},
  // @ts-expect-error without state: the handler gets no state argument
  emit: (_state: number, _batch: string) => Effect.succeed({}),
});
// @ts-expect-error a stateless declaration has no state source
void declared.state;
