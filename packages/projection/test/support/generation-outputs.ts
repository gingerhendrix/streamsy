import { Effect, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, FoldByKey, Projection, StateTable, type Emitted } from "@streamsy/projection";
import { Fact, fold, Row } from "./fold-by-key.ts";

export class Facts extends Streamsy.json<Facts>()("G1/Facts", "g1/facts/:workspace", {
  params: { workspace: Schema.String },
  schema: Fact,
}) {}

export class Numbers extends Streamsy.json<Numbers>()(
  "G1/Numbers",
  "g1/numbers/:workspace/g/:rev",
  {
    params: { workspace: Schema.String, rev: Schema.FiniteFromString },
    schema: Schema.Finite,
  },
) {}

export class Rows extends Streamsy.state<Rows>()("G1/Rows", "g1/rows/:workspace/g/:rev", {
  params: { workspace: Schema.String, rev: Schema.FiniteFromString },
  collections: { sum: { schema: Row, key: "id" } },
}) {}

export class Accepted extends Streamsy.wake<Accepted>()(
  "G1/Accepted",
  "g1/accepted/:workspace/g/:epoch",
  { params: { workspace: Schema.String, epoch: Schema.FiniteFromString } },
) {}

export const table = StateTable.define(Rows);

export const params = { workspace: "one" };

export const declaration = {
  params: { workspace: Schema.String },
  inputs: { facts: Facts },
};

export const output = Projection.perGeneration(Numbers, "rev");

export const accepted = Projection.perGeneration(Accepted, "epoch");

export const failedHandler = new Error("generation handler failed");

export const numbers = <R>(
  commit: Commit.Commit<Emitted<{ readonly numbers: never }>, never, R>,
  generation: number,
  fail = false,
) =>
  Projection.emit({
    ...declaration,
    id: "g1-numbers",
    generation,
    outputs: { numbers: output },
    accepted,
    commit,
    emit: ({ item }) => (fail ? Effect.fail(failedHandler) : { numbers: [item.n] }),
  });

export const sums = (generation: number) =>
  Projection.process({
    ...declaration,
    id: "g1-sums",
    generation,
    outputs: { rows: Projection.perGeneration(Rows, "rev") },
    commit: Commit.transaction,
    process: FoldByKey.process(table, {
      output: "rows",
      collection: "sum",
      key: (record) => record.id,
      fold,
    }),
  });
