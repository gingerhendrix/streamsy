import { Effect, Schema, type Scope } from "effect";
import {
  StreamFamily,
  StreamRef,
  Streams,
  provides,
  type StreamsReader,
  type StreamsWriter,
} from "@streamsy/core";
import { Checkpoints, Output, Projection, State, type ProjectionFault } from "@streamsy/projection";
// @ts-expect-error Host is no longer a public projection requirement.
import type { Host } from "@streamsy/projection";
export type RemovedHost = Host;
class Input extends StreamFamily.json<Input>()("ProjectionTypes/Input", "types/input/:id", {
  params: { id: Schema.String },
  schema: Schema.Finite,
}) {}
class Other extends StreamFamily.json<Other>()("ProjectionTypes/Other", "types/other/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}
class Result extends StreamFamily.bytes<Result>()("ProjectionTypes/Result", "types/result/:id", {
  params: { id: Schema.String },
}) {}
const fused = Projection.make({
  id: "types/fused",
  input: Input.at({ id: "a" }),
  process: () => Effect.void,
});
const pinned = Projection.stream({
  id: "types/pinned",
  input: Input.at({ id: "a" }),
  output: Other.at({ id: "a" }),
  process: (batch) => Effect.succeed(batch.input.items.map(String)),
});
const family = Projection.family({
  id: "types/named",
  params: { id: Schema.String },
  inputs: { input: Input, other: Other },
  outputs: { result: Output.stream(Schema.Finite, { stream: Result }) },
  process: (batch) => Effect.succeed({ result: batch.input.items }),
});
const named = family.member({ id: "a" });
const fusedRun = Projection.run(fused);
const pinnedRun = Projection.run(pinned);
const namedRun = Projection.serialized(named);
const namedWatch = Projection.onChange(family, [{ id: "a" }]);
const namedFollow = Projection.follow(named);
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;
export type Checks = [
  Assert<Same<Effect.Services<typeof fusedRun>, Input | Checkpoints>>,
  Assert<Same<Effect.Services<typeof pinnedRun>, Input | Other | Checkpoints>>,
  Assert<Same<Effect.Services<typeof namedRun>, Input | Other | Result | State | Checkpoints>>,
  Assert<
    Same<
      Effect.Services<typeof namedWatch>,
      Input | Other | Result | State | Checkpoints | Scope.Scope
    >
  >,
  Assert<
    Same<
      Effect.Services<typeof namedFollow>,
      Input | Other | Result | State | Checkpoints | Scope.Scope
    >
  >,
  Assert<Same<Extract<Effect.Services<typeof namedRun>, StreamsReader | StreamsWriter>, never>>,
];
const missing = namedRun.pipe(Effect.provide(provides(Streams.layerMemory(), Input, Other)));
// oxlint-disable effecttsgo/missing-effect-context -- This negative test deliberately omits Result.
// @ts-expect-error the declared output family must still be provided.
const incomplete: Effect.Effect<unknown, ProjectionFault, Checkpoints | State> = missing;
void incomplete;
// oxlint-enable effecttsgo/missing-effect-context
Projection.make({
  id: "bare",
  // @ts-expect-error plain refs do not carry a family key
  input: StreamRef.json("plain", { schema: Schema.Finite }),
  process: () => Effect.void,
});
