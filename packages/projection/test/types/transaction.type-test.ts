import { Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Commit, Projection, Transaction } from "@streamsy/projection";

class Input extends Streamsy.json<Input>()("transaction-types/Input", "transaction-types/input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Output extends Streamsy.json<Output>()(
  "transaction-types/Output",
  "transaction-types/output/:generation",
  {
    params: { generation: Schema.FiniteFromString },
    schema: Schema.Finite,
  },
) {}

const definition = {
  id: "transaction-output-types",
  params: {},
  generation: 1,
  inputs: { input: Input },
  outputs: { output: Projection.perGeneration(Output, "generation") },
};

const sql = () => Effect.asVoid(SqlClient.SqlClient);

const transactionCommit = Projection.process({
  ...definition,
  commit: Commit.transaction,
  process: (_batch, _unit, { outputs }) => {
    const id: ReturnType<typeof Output.at>["id"] = outputs.output.id;
    void id;

    return Effect.void;
  },
});

const atLeastOnce = { ...definition, commit: Commit.atLeastOnce, process: () => Effect.void };

// @ts-expect-error At-least-once commits cannot declare outputs.
Projection.process(atLeastOnce);

const transactionCommitWithSql = Projection.process({
  ...definition,
  commit: Commit.transaction,
  process: sql,
});

const transactionRun = Projection.run(transactionCommit.at({}));

const transactionRunWithSql = Projection.run(transactionCommitWithSql.at({}));

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

type MemoryRequirements = Input | Output | CheckpointStore | Transaction;

export type TransactionCommitChecks = [
  Assert<Same<Effect.Services<typeof transactionRun>, MemoryRequirements>>,
  Assert<
    Same<Effect.Services<typeof transactionRunWithSql>, MemoryRequirements | SqlClient.SqlClient>
  >,
];

// oxlint-disable effecttsgo/missing-effect-context -- The transaction commit must leave the SQL handler requirement visible.
// @ts-expect-error A memory host cannot run a handler requiring SqlClient.
const runSqlHandlerOnMemory: typeof transactionRun = transactionRunWithSql;

// oxlint-enable effecttsgo/missing-effect-context

void runSqlHandlerOnMemory;
