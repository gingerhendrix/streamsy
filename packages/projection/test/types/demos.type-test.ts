import { Context, Effect, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { DurableState, Streamsy } from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
} from "@streamsy/projection";

// The five declarations from the design spike, using the actual package API.
class StoryEvents extends Streamsy.json<StoryEvents>()("demos/Stories", "stories", {
  params: {},
  schema: Schema.Struct({ id: Schema.String, title: Schema.String }),
}) {}
const StoryRows = Schema.Record(Schema.String, Schema.String);
const newestRows = Projection.fold({
  id: "hn-newest-rows",
  params: {},
  inputs: { input: StoryEvents },
  state: { schema: StoryRows, initial: {} },
  fold: (rows, { item }) => ({ ...rows, [item.id]: item.title }),
});
class GameEvents extends Streamsy.json<GameEvents>()("demos/Game", "game/:gameId", {
  params: { gameId: Schema.String },
  schema: Schema.String,
}) {}
const board = Projection.fold({
  id: "hex-board",
  params: { gameId: Schema.String },
  inputs: { input: GameEvents },
  state: {
    schema: Schema.Struct({ gameId: Schema.String, through: Schema.String }),
    initial: ({ gameId }) => ({ gameId, through: "0" }),
  },
  fold: (state, _entry, unit) => ({ ...state, through: unit.ranges.input!.nextOffset }),
});
const params = { workspaceId: Schema.String };
class Events extends Streamsy.json<Events>()("demos/Events", "events/:workspaceId", {
  params,
  schema: Schema.Struct({ issueId: Schema.String }),
}) {}
class Labels extends Streamsy.json<Labels>()("demos/Labels", "labels/:workspaceId", {
  params,
  schema: Schema.Struct({ labelId: Schema.String }),
}) {}
class CatalogMismatch extends Schema.TaggedError<CatalogMismatch>()("CatalogMismatch", {}) {}
class Catalog extends Context.Service<
  Catalog,
  { readonly check: (workspaceId: string) => Effect.Effect<void, CatalogMismatch> }
>()("demos/Catalog") {}
const issueRows = Projection.process({
  id: "issue-rows",
  params,
  inputs: { events: Events, labels: Labels },
  commit: Commit.transaction,
  process: Projection.each((entry, unit) =>
    Effect.gen(function* () {
      yield* (yield* Catalog).check(unit.params.workspaceId);
      const sql = yield* SqlClient.SqlClient;
      const id = entry.input === "events" ? entry.item.issueId : entry.item.labelId;
      yield* sql`INSERT INTO rows (id) VALUES (${id})`;
    }),
  ),
});
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
  schema: Schema.Number,
}) {}
class Doubled extends Streamsy.json<Doubled>()("demos/Doubled", "doubled", {
  params: {},
  schema: Schema.Number,
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
const newestRun = Projection.run(newestRows.at({}));
const boardRun = Projection.run(board.at({ gameId: "a" }));
const rowsRun = Projection.run(issueRows.at({ workspaceId: "a" }));
const trackerRun = Projection.run(tracker.at({ workspaceId: "a" }));
const minimalRun = Projection.run(minimal.at({}));
const read = Projection.read(newestRows.at({})).pipe(Effect.map(({ state }) => state));
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;
export type Checks = [
  Assert<Same<Effect.Services<typeof newestRun>, StoryEvents | CheckpointStore>>,
  Assert<Same<Effect.Services<typeof boardRun>, GameEvents | CheckpointStore>>,
  Assert<
    Same<Effect.Services<typeof rowsRun>, Events | Labels | Catalog | CheckpointStore | Transaction>
  >,
  Assert<
    Same<
      Effect.Services<typeof trackerRun>,
      Events | Board | Transitions | Catalog | CheckpointStore
    >
  >,
  Assert<
    Same<Effect.Services<typeof minimalRun>, Numbers | Doubled | CheckpointStore | Transaction>
  >,
  Assert<Same<Effect.Services<typeof read>, CheckpointStore>>,
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
    state: { schema: Schema.Number, initial: 0 },
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
  state: { schema: Schema.Number, initial: 0 },
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
