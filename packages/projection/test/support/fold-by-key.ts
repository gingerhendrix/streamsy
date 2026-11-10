import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, FoldByKey, Projection, StateTable } from "@streamsy/projection";

export const Fact = Schema.Struct({ id: Schema.String, n: Schema.Finite });

export type Fact = typeof Fact.Type;

export const Row = Schema.Struct({ id: Schema.String, count: Schema.Finite });

export type Row = typeof Row.Type;

export class Facts extends Streamsy.json<Facts>()(
  "FoldByKey/Facts",
  "fold-facts/:workspace/:source",
  {
    params: { workspace: Schema.String, source: Schema.String },
    schema: Fact,
  },
) {}

export class Workspace extends Streamsy.state<Workspace>()(
  "FoldByKey/Workspace",
  "fold-state/:workspace",
  {
    params: { workspace: Schema.String },
    collections: {
      sum: { schema: Row, key: "id" },
      status: { schema: Schema.Struct({ id: Schema.String, label: Schema.String }), key: "id" },
    },
  },
) {}

export const Table = StateTable.define(Workspace);

export const fold = (previous: Row | undefined, record: Fact): Row => ({
  id: record.id,
  count: (previous?.count ?? 0) + record.n,
});

export const sums = (id = "fold-sums", generation = 1) =>
  Projection.process({
    id,
    generation,
    params: { workspace: Schema.String, source: Schema.String },
    inputs: { facts: Facts },
    outputs: { state: Workspace },
    commit: Commit.transaction,
    process: FoldByKey.process(Table, {
      output: "state",
      collection: "sum",
      key: (record) => record.id,
      fold,
    }),
  });
