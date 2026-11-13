import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";

class Source extends Streamsy.json<Source>()("AcceptedTypes/Source", "source", {
  params: {},
  schema: Schema.Finite,
}) {}

class Accepted extends Streamsy.wake<Accepted>()("AcceptedTypes/Wake", "accepted/:id", {
  params: { id: Schema.String },
}) {}

const bound = Projection.fold({
  id: "types",
  params: { id: Schema.String },
  inputs: { source: Source },
  accepted: Accepted,
  commit: Commit.transaction,
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state, entry) => state + entry.item,
}).at({ id: "one" });

// The kernel's output service is required even though handlers do not use it.
export const run = Projection.run(bound);

type Assert<T extends true> = T;

export type OutputServiceRequired = Assert<
  Accepted extends Effect.Services<typeof run> ? true : false
>;

Projection.process({
  id: "wrong-type",
  params: { id: Schema.FiniteFromString },
  inputs: {},
  commit: Commit.transaction,
  // @ts-expect-error accepted params retain their decoded type
  accepted: Accepted,
  process: () => Effect.void,
});

Projection.process({
  id: "wrong-class",
  params: {},
  inputs: {},
  commit: Commit.transaction,
  // @ts-expect-error accepted requires wake collections
  accepted: Source,
  process: () => Effect.void,
});

// @ts-expect-error accepted requires an explicit supported commit strategy
Projection.fold({
  id: "missing-commit",
  params: { id: Schema.String },
  inputs: { source: Source },
  accepted: Accepted,
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state) => state,
});

Projection.fold({
  id: "wrong-commit",
  params: { id: Schema.String },
  inputs: { source: Source },
  accepted: Accepted,
  // @ts-expect-error checkpoint does not publish accepted rows
  commit: Commit.checkpoint,
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state) => state,
});
