import { expect } from "vitest";
import { Effect, Layer, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, Projection, type RunnerStatus } from "@streamsy/projection";
import { memoryHost } from "./memory-host.ts";

export class Source extends Streamsy.json<Source>()("Registry/Source", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

export class Other extends Streamsy.json<Other>()("Registry/Other", "other/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

export class NumericSource extends Streamsy.json<NumericSource>()(
  "Registry/NumericSource",
  "numeric/:id",
  {
    params: { id: Schema.FiniteFromString },
    schema: Schema.String,
  },
) {}

const local = memoryHost({ readLimit: 1 });

export const host = Protocol.bind(local, Streamsy.group(Source, Other, NumericSource)).pipe(
  Layer.provideMerge(local),
);

export const projection = (id: string, process: Effect.Effect<void> = Effect.void) =>
  Projection.process({
    id,
    params: { id: Schema.String },
    inputs: { source: Source },
    commit: Commit.atLeastOnce,
    process: () => process,
  });

export const only = (statuses: ReadonlyArray<RunnerStatus>) => {
  expect(statuses).toHaveLength(1);

  return statuses[0]!;
};
