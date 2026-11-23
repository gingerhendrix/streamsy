import { Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  OwnerTransaction,
  Projection,
  Transaction,
} from "@streamsy/projection";

class Input extends Streamsy.json<Input>()("owner-types/Input", "owner-types/input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Output extends Streamsy.json<Output>()(
  "owner-types/Output",
  "owner-types/output/:generation",
  {
    params: { generation: Schema.FiniteFromString },
    schema: Schema.Finite,
  },
) {}

const definition = {
  id: "owner-output-types",
  params: {},
  generation: 1,
  inputs: { input: Input },
  outputs: { output: Projection.perGeneration(Output, "generation") },
};

const sql = () => Effect.asVoid(SqlClient.SqlClient);

const sqlCommit = Projection.process({ ...definition, commit: Commit.transaction, process: sql });

const ownerCommit = Projection.process({
  ...definition,
  commit: Commit.ownerTransaction,
  process: (_batch, _unit, { outputs }) => {
    const id: ReturnType<typeof Output.at>["id"] = outputs.output.id;
    void id;

    return Effect.void;
  },
});

const atLeastOnce = { ...definition, commit: Commit.atLeastOnce, process: () => Effect.void };

// @ts-expect-error At-least-once commits cannot declare outputs.
Projection.process(atLeastOnce);

const ownerCommitWithSql = Projection.process({
  ...definition,
  commit: Commit.ownerTransaction,
  process: sql,
});

const sqlRun = Projection.run(sqlCommit.at({}));

const ownerRun = Projection.run(ownerCommit.at({}));

const ownerRunWithSql = Projection.run(ownerCommitWithSql.at({}));

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

type MemoryRequirements = Input | Output | CheckpointStore | OwnerTransaction;

export type OwnerCommitChecks = [
  Assert<Same<Effect.Services<typeof sqlRun>, Input | Output | CheckpointStore | Transaction>>,
  Assert<Same<Effect.Services<typeof ownerRun>, MemoryRequirements>>,
  Assert<Same<Effect.Services<typeof ownerRunWithSql>, MemoryRequirements | SqlClient.SqlClient>>,
];

// oxlint-disable effecttsgo/missing-effect-context -- The owner commit must leave the SQL handler requirement visible.
// @ts-expect-error A memory host cannot run a handler requiring SqlClient.
const runSqlHandlerOnMemory: typeof ownerRun = ownerRunWithSql;

// oxlint-enable effecttsgo/missing-effect-context

void runSqlHandlerOnMemory;
