import { layer } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect } from "vitest";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { DurableState, MemoryStorage, Streamsy } from "@streamsy/core";
import {
  CheckpointStore,
  Projection,
  ProjectionFault,
  StateTable,
  Transaction,
} from "@streamsy/projection";
import { Facts, Table, Workspace, sums } from "./support/fold-by-key.ts";

// The streams and checkpoints live in memory, so the SQL client is not the storage owner's.
const host = Layer.effectDiscard(Effect.flatMap(SqlClient.SqlClient, StateTable.migrate)).pipe(
  Layer.provideMerge(
    Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
      Layer.provideMerge(MemoryStorage.layer(Streamsy.group(Facts, Workspace))),
      Layer.merge(SqliteClient.layer({ filename: ":memory:" })),
    ),
  ),
);

const params = { workspace: "main", source: "events" };

const workspace = { workspace: "main" };

const rejected = { _tag: "ProjectionFault", reason: "unsupported-composition", retryable: false };

const rows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  return {
    rows: yield* sql`SELECT * FROM streamsy_state_rows ORDER BY row_key`,
    heads: yield* sql`SELECT * FROM streamsy_state_heads`,
  };
});

const seedRow = Effect.gen(function* () {
  const state = yield* Workspace;
  yield* state.create(workspace);
  yield* state.append(
    workspace,
    DurableState.changes(Workspace.at(workspace), {}, [
      DurableState.upsert("sum", { id: "a", count: 5 }),
    ]),
  );
});

layer(host, { buildPerTest: true })((it) => {
  it.effect("FoldByKey does not fold over rows outside the owner transaction", () =>
    Effect.gen(function* () {
      const facts = yield* Facts;
      const state = yield* Workspace;
      const store = yield* CheckpointStore;
      yield* facts.create(params);
      yield* facts.append(params, [{ id: "a", n: 1 }]);

      const binding = sums().at(params);

      const lost = new ProjectionFault({
        phase: "checkpoint",
        reason: "token-conflict",
        message: "lost compare-and-set",
      });

      const first = yield* Projection.run(binding).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () => Effect.fail(lost),
        }),
        Effect.flip,
      );

      expect(first).toMatchObject(rejected);
      expect(yield* Projection.run(binding).pipe(Effect.flip)).toMatchObject(rejected);
      expect(yield* rows).toEqual({ rows: [], heads: [] });
      expect(yield* state.head(workspace).pipe(Effect.flip)).toMatchObject({
        _tag: "StreamNotFound",
      });
      expect((yield* store.load(binding)).token).toBe("0");
    }),
  );

  for (const [name, operation] of [
    ["sync", Table.at(workspace).sync()],
    ["a table run", Projection.run(Table.at(workspace))],
    ["rebuild", StateTable.rebuild(Table.at(workspace))],
    ["clear", StateTable.clear(Table.at(workspace))],
  ] as const)
    it.effect(`StateTable rejects ${name} without a transaction on its client`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const tx = yield* Transaction;

        yield* seedRow;
        yield* sql.withTransaction(Table.at(workspace).sync());
        const before = yield* rows;
        expect(before.rows).toHaveLength(1);

        expect(yield* operation.pipe(Effect.flip)).toMatchObject(rejected);
        expect(yield* tx.withTransaction(operation).pipe(Effect.flip)).toMatchObject(rejected);
        expect(yield* rows).toEqual(before);
      }),
    );
});
