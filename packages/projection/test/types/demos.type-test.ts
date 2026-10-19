import { Context, Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
} from "@streamsy/projection";

// The issue tracker and minimal declarations from the design spike, using the actual package API.
const params = { workspaceId: Schema.String };

class Events extends Streamsy.json<Events>()("demos/Events", "events/:workspaceId", {
  params,
  schema: Schema.Struct({ issueId: Schema.String }),
}) {}

class CatalogMismatch extends Schema.TaggedError<CatalogMismatch>()("CatalogMismatch", {}) {}

class Catalog extends Context.Service<
  Catalog,
  { readonly check: (workspaceId: string) => Effect.Effect<void, CatalogMismatch> }
>()("demos/Catalog") {}

class Board extends Streamsy.state<Board>()("demos/Board", "board/:workspaceId", {
  params,
  collections: { board: { schema: Schema.Struct({ issueId: Schema.String }), key: "issueId" } },
}) {}

class Transitions extends Streamsy.json<Transitions>()(
  "demos/Transitions",
  "transitions/:workspaceId",
  { params, schema: Schema.String },
) {}

const tracker = Projection.mapAccumBatch({
  id: "issue-tracker",
  params,
  inputs: { events: Events },
  outputs: {
    board: Board,
    transitions: Transitions,
  },
  state: {
    schema: Schema.Struct({ workspaceId: Schema.String, count: Schema.Int }),
    initial: ({ workspaceId }) => ({ workspaceId, count: 0 }),
  },
  commit: Commit.producer,
  mapAccumBatch: (previous, batch) =>
    Effect.gen(function* () {
      yield* (yield* Catalog).check(previous.workspaceId);

      return [
        { ...previous, count: previous.count + batch.events.items.length },
        {
          board: batch.events.items.map((row) => DurableState.upsert("board", row)),
          transitions: batch.events.items.map((e) => e.issueId),
        },
      ] as const;
    }),
});

class Numbers extends Streamsy.json<Numbers>()("demos/Numbers", "numbers", {
  params: {},
  schema: Schema.Finite,
}) {}

class Doubled extends Streamsy.json<Doubled>()("demos/Doubled", "doubled", {
  params: {},
  schema: Schema.Finite,
}) {}

const outputs = { output: Doubled };

const minimal = Projection.emit({
  id: "doubled",
  params: {},
  inputs: { input: Numbers },
  outputs,
  commit: Commit.transaction,
  emit: ({ item }) => ({ output: [item * 2] }),
});

const trackerRun = Projection.run(tracker.at({ workspaceId: "a" }));

const minimalRun = Projection.run(minimal.at({}));

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

export type Checks = [
  Assert<
    Same<Effect.Services<typeof minimalRun>, Numbers | Doubled | CheckpointStore | Transaction>
  >,
  Assert<Same<Effect.Error<typeof trackerRun>, CatalogMismatch | ProjectionFault>>,
];

const invalid = () => {
  Projection.process({
    id: "wrong-process",
    params: {},
    inputs: { input: Numbers },
    // @ts-expect-error checkpoint lands Folded, not void
    commit: Commit.checkpoint,
    process: () => Effect.void,
  });
  Projection.emit({
    id: "wrong-emit",
    params: {},
    inputs: { input: Numbers },
    outputs,
    // @ts-expect-error checkpoint cannot discard outputs
    commit: Commit.checkpoint,
    emit: ({ item }) => ({ output: [item] }),
  });
  // @ts-expect-error emit must name its guarantee
  Projection.emit({
    id: "missing-commit",
    params: {},
    inputs: { input: Numbers },
    outputs,
    emit: () => ({ output: [1] }),
  });
  Projection.mapAccum({
    id: "wrong-accum",
    params: {},
    inputs: { input: Numbers },
    outputs,
    state: { schema: Schema.Finite, initial: 0 },
    // @ts-expect-error Accumulated has its own tag, so a value-only Commit cannot drop outputs
    commit: Commit.checkpoint,
    mapAccum: (state, { item }) => [state + item, { output: [item] }],
  });
  Projection.emit({
    id: "wrong-at-least-once",
    params: {},
    inputs: { input: Numbers },
    outputs,
    // @ts-expect-error atLeastOnce lands void only
    commit: Commit.atLeastOnce,
    emit: () => ({}),
  });
};

void invalid;

// prepare runs outside the Commit-provided SQL context. Its SQL requirement must remain.
const prepared = Projection.process({
  id: "prepared-types",
  params: {},
  inputs: { input: Numbers },
  commit: Commit.transaction,
  prepare: (_batch, unit) =>
    Effect.gen(function* () {
      yield* SqlClient.SqlClient;
      yield* (yield* Catalog).check(unit.key);

      return "prepared";
    }),
  process: (_batch, _unit, value) =>
    Effect.gen(function* () {
      const check: string = value;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`SELECT ${check}`;
    }),
});

const preparedRun = Projection.run(prepared.at({}));

export type PreparedChecks = [
  Assert<
    Same<
      Effect.Services<typeof preparedRun>,
      Numbers | Catalog | SqlClient.SqlClient | CheckpointStore | Transaction
    >
  >,
];

const sqlFold = Projection.fold({
  id: "sql-fold",
  params: {},
  inputs: { input: Numbers },
  state: { schema: Schema.Finite, initial: 0 },
  commit: Commit.transaction,
  fold: (previous, { item }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO totals (value) VALUES (${previous + item})`;

      return previous + item;
    }),
});

const sqlFoldRun = Projection.run(sqlFold.at({}));

export type SqlFoldChecks = [
  Assert<Same<Effect.Services<typeof sqlFoldRun>, Numbers | CheckpointStore | Transaction>>,
];
