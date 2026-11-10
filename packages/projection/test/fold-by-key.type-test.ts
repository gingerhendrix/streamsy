import { Effect, Schema } from "effect";
import {
  FoldByKey,
  FoldByKeyError,
  Projection,
  Commit,
  type Unit,
  type CheckpointStore,
  type ProjectionFault,
  type Transaction,
} from "@streamsy/projection";
import { Facts, Workspace, Table, fold, sums, type Fact } from "./support/fold-by-key.ts";

export const named = sums();

export const expanded = Projection.process({
  id: "expanded-types",
  params: { workspace: Schema.String, source: Schema.String },
  inputs: { facts: Facts },
  outputs: { state: Workspace },
  commit: Commit.transaction,
  process: FoldByKey.process(Table, {
    output: "state",
    collection: "sum",
    records: (item) => [{ ref: item.id, value: item.n }],
    key: (record, unit) => {
      const params: { readonly workspace: string; readonly source: string } = unit.params;
      void params;

      return record.ref;
    },
    fold: (_previous, record) => Effect.succeed({ id: record.ref, count: record.value }),
  }),
});

const missing = {
  output: "state",
  collection: "missing" as const,
  key: (record: Fact) => record.id,
  fold,
};

// @ts-expect-error a collection must be declared on the StateTable
FoldByKey.process(Table, missing);

const wrongRow = {
  output: "state",
  collection: "sum" as const,
  key: (record: Fact) => record.id,
  fold: () => ({ id: "a", label: "wrong" }),
};

// @ts-expect-error a sum fold cannot return a status row
FoldByKey.process(Table, wrongRow);

export const fallible = Projection.process({
  id: "fallible-types",
  params: { workspace: Schema.String, source: Schema.String },
  inputs: { facts: Facts },
  outputs: { state: Workspace },
  commit: Commit.transaction,
  process: FoldByKey.process(Table, {
    output: "state",
    collection: "sum",
    key: (record) => record.id,
    fold: (previous, record, unit) => {
      const check: Unit<{ readonly workspace: string; readonly source: string }> = unit;
      void check;

      return record.n < 0 ? Effect.fail("negative" as const) : fold(previous, record);
    },
  }),
});

const run = Projection.run(fallible.at({ workspace: "a", source: "a" }));

type Assert<A extends true> = A;

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

const namedRun = Projection.run(named.at({ workspace: "a", source: "a" }));

// A caller can name the handler in a signature.
export const handler: FoldByKey.Handler<
  typeof Workspace.collections,
  "state",
  { readonly facts: ReturnType<typeof Facts.at> },
  { readonly workspace: string; readonly source: string },
  never,
  never,
  Workspace
> = FoldByKey.process(Table, {
  output: "state",
  collection: "sum",
  key: (record) => record.id,
  fold,
});

export type ErrorChecks = [
  Assert<"negative" extends Effect.Error<typeof run> ? true : false>,
  Assert<FoldByKeyError extends Effect.Error<typeof run> ? true : false>,
  // Wire, SQL and schema failures of the handler are faults.
  Assert<Same<Effect.Error<ReturnType<typeof handler>>, FoldByKeyError | ProjectionFault>>,
  Assert<Same<Effect.Success<ReturnType<typeof handler>>, void>>,
  // The error class has one public path.
  Assert<"FoldByKeyError" extends keyof typeof FoldByKey ? false : true>,
  Assert<Same<Effect.Services<typeof namedRun>, CheckpointStore | Transaction | Facts | Workspace>>,
];

Projection.process({
  id: "missing-output-param",
  params: { source: Schema.String },
  inputs: {},
  // @ts-expect-error the output requires workspace
  outputs: { state: Workspace },
  commit: Commit.transaction,
  process: () => Effect.void,
});
