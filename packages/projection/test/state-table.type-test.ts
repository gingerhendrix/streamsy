import { Effect, Schema } from "effect";
import { SqlClient, type SqlError } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import {
  CheckpointStore,
  Projection,
  ProjectionWakeup,
  StateTable,
  Transaction,
  type ProjectionFault,
} from "@streamsy/projection";

class Workspace extends Streamsy.state<Workspace>()("StateTableTypes/Workspace", "workspaces/:id", {
  params: { id: Schema.FiniteFromString },
  collections: {
    issue: { schema: Schema.Struct({ id: Schema.String, count: Schema.Finite }), key: "id" },
  },
}) {}

const declaration = StateTable.define(Workspace);

const table = declaration.at({ id: 1 });

const get = table.get("issue", "a");

const many = table.getMany("issue", ["a"]);

const list = table.list("issue");

const run = Projection.run(table);

const sync = table.sync();

const rebuild = StateTable.rebuild(table);

export const follow = Projection.follow(table);

export const attach = Projection.attach(declaration, { durable: true });

export const wakeup = ProjectionWakeup.attach(declaration);

export const reset = StateTable.reset(table);

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Codecs = { readonly id: typeof Schema.FiniteFromString };

// A caller can name the declaration and the bound table in a signature.
export const named: StateTable.Declaration<Codecs, typeof Workspace.collections, Workspace> =
  declaration;

export const bound: StateTable.Bound<typeof Workspace.collections, Workspace> = table;

export const countOf = (issues: StateTable.Bound<typeof Workspace.collections, Workspace>) =>
  Effect.map(issues.get("issue", "a"), ({ value }) => value?.count);

type Assert<A extends true> = A;

export type Checks = [
  Assert<
    Same<
      Effect.Success<typeof get>,
      { offset: string; value: { readonly id: string; readonly count: number } | undefined }
    >
  >,
  Assert<
    Same<
      Effect.Success<typeof many>["rows"],
      ReadonlyMap<string, { readonly id: string; readonly count: number }>
    >
  >,
  Assert<
    Same<
      Effect.Success<typeof list>["rows"][number]["value"],
      { readonly id: string; readonly count: number }
    >
  >,
  Assert<Same<Effect.Services<typeof get>, SqlClient.SqlClient>>,
  Assert<Same<Effect.Services<typeof run>, Workspace | CheckpointStore | Transaction>>,
  Assert<Same<Effect.Services<typeof sync>, Workspace | Transaction>>,
  Assert<Same<Effect.Services<typeof rebuild>, Workspace | Transaction>>,
  Assert<Same<Effect.Success<typeof sync>, void>>,
  Assert<Same<Effect.Error<typeof sync>, ProjectionFault>>,
  Assert<Same<Effect.Error<typeof rebuild>, ProjectionFault>>,
  Assert<Same<Effect.Error<typeof get>, Schema.SchemaError | SqlError.SqlError>>,
];

// @ts-expect-error params retain the State class's decoded types
declaration.at({ id: "1" });

// @ts-expect-error unknown collection
export const unknownGet = table.get("missing", "a");

// @ts-expect-error unknown collection
export const unknownMany = table.getMany("missing", []);

// @ts-expect-error unknown collection
export const unknownList = table.list("missing");

class Facts extends Streamsy.json<Facts>()("StateTableTypes/Facts", "facts", {
  params: {},
  schema: Schema.String,
}) {}

// @ts-expect-error only State classes have collections
StateTable.define(Facts);
