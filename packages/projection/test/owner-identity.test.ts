/* oxlint-disable effecttsgo/strict-effect-provide -- Each binding runs with the context of its own SQLite host. */
import { it } from "@effect/vitest";
import { CommitBoundary } from "@streamsy/core";
import { CheckpointStore, Commit, Projection, ProjectionFault } from "@streamsy/projection";
import { Context, Effect, Exit, Layer, Match, Option, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { expect } from "vitest";
import { withFamilies } from "./support/families.ts";
import { Facts } from "./support/generation-outputs.ts";
import { readAll } from "./support/scenarios.ts";
import { layerSqlite } from "./support/sqlite.ts";

const params = { workspace: "owner-identity" };

const buildHost = () =>
  Layer.build(withFamilies(layerSqlite(), Facts)).pipe(
    Effect.tap((context) =>
      Effect.gen(function* () {
        const sql = Context.get(context, SqlClient.SqlClient);
        const facts = Context.get(context, Facts);
        yield* sql`CREATE TABLE app_rows (id INTEGER PRIMARY KEY)`;
        yield* facts.create(params);
        yield* facts.append(params, [{ id: "a", n: 1 }]);
      }),
    ),
  );

const declaration = {
  generation: 1,
  params: { workspace: Schema.String },
  inputs: { facts: Facts },
};

for (const operation of ["run", "retire", "forget"] as const) {
  it.effect(`a foreign projection ${operation} is atomic inside a transactional handler`, () =>
    Effect.gen(function* () {
      // Build separate Layers so the memo map allocates a separate SQLite connection for each.
      const left = yield* buildHost();
      const right = yield* buildHost();
      const leftSql = Context.get(left, SqlClient.SqlClient);
      const rightSql = Context.get(right, SqlClient.SqlClient);
      const leftStore = Context.get(left, CheckpointStore);
      const rightStore = Context.get(right, CheckpointStore);

      const cleanupFailure = new ProjectionFault({
        phase: "checkpoint",
        reason: "storage-failure",
        message: "after cleanup write",
      });

      let failCleanup = true;

      const foreign = Projection.process({
        ...declaration,
        id: "foreign-owner",
        commit:
          operation === "run"
            ? Commit.transaction
            : Commit.transactionWithCleanup((_, sql) =>
                sql`INSERT INTO app_rows VALUES (2)`.pipe(
                  Effect.orDie,
                  Effect.andThen(() => (failCleanup ? Effect.fail(cleanupFailure) : Effect.void)),
                ),
              ),
        process: () =>
          operation === "run"
            ? SqlClient.SqlClient.use((sql) =>
                sql`INSERT INTO app_rows VALUES (2)`.pipe(Effect.asVoid),
              )
            : Effect.void,
      });

      const bound = foreign.at(params);

      if (operation !== "run") yield* Projection.run(bound).pipe(Effect.provideContext(right));
      const before = yield* rightStore.load(bound);

      const foreignOperation = Match.value(operation).pipe(
        Match.when("run", () => Projection.run(bound)),
        Match.when("retire", () => Projection.retire(foreign, params, { generation: 1 })),
        Match.when("forget", () => Projection.forget(bound)),
        Match.exhaustive,
        Effect.asVoid,
        Effect.provideContext(right),
      );

      const outer = Projection.process({
        ...declaration,
        id: "outer-owner",
        commit: Commit.transaction,
        process: () =>
          leftSql`INSERT INTO app_rows VALUES (1)`.pipe(
            Effect.andThen(foreignOperation),
            Effect.andThen(Effect.fail("after foreign projection")),
          ),
      }).at(params);

      const runOuter = Projection.run(outer).pipe(Effect.provideContext(left));

      if (operation === "run")
        yield* rightSql.unsafe(
          "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON streamsy_owner_records BEGIN SELECT RAISE(ABORT, 'after handler write'); END",
        );

      const failedForeign = yield* runOuter.pipe(Effect.flip);
      expect(failedForeign).toMatchObject(
        operation === "run" ? { reason: "storage-failure" } : cleanupFailure,
      );
      expect(yield* leftSql`SELECT id FROM app_rows`).toEqual([]);
      expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([]);
      expect((yield* leftStore.load(outer)).token).toBe("0");
      expect(yield* rightStore.load(bound)).toEqual(before);

      if (operation === "run") yield* rightSql`DROP TRIGGER fail_checkpoint`;
      failCleanup = false;
      expect(yield* runOuter.pipe(Effect.flip)).toBe("after foreign projection");
      expect(yield* leftSql`SELECT id FROM app_rows`).toEqual([]);
      expect((yield* leftStore.load(outer)).token).toBe("0");
      expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([{ id: 2 }]);

      if (operation === "retire")
        expect(yield* rightStore.load(bound).pipe(Effect.flip)).toMatchObject({
          reason: "retired-generation",
        });
      else expect((yield* rightStore.load(bound)).token).toBe(operation === "run" ? "1" : "0");
    }),
  );
}

it.effect(
  "a non-transactional projection saves its append and checkpoint inside a foreign boundary",
  () =>
    Effect.gen(function* () {
      const left = yield* buildHost();
      const right = yield* buildHost();
      const leftBoundary = Context.get(left, CommitBoundary);
      const facts = Context.get(right, Facts);
      const store = Context.get(right, CheckpointStore);
      const output = { workspace: "non-transactional-output" };
      yield* facts.create(output);
      let append = false;

      const projection = Projection.process({
        ...declaration,
        id: "non-transactional-owner",
        commit: Commit.atLeastOnce,
        process: () =>
          Effect.gen(function* () {
            if (!append) return yield* Effect.fail("before append");
            yield* facts.append(output, [{ id: "b", n: 2 }]).pipe(Effect.orDie);

            return yield* Effect.void;
          }),
      }).at(params);

      const run = Projection.run(projection).pipe(Effect.provideContext(right));
      const contents = readAll(Facts.at(output)).pipe(Effect.provideContext(right));

      // Resume a pinned unit so its next checkpoint write follows the append.
      expect(yield* run.pipe(Effect.flip)).toBe("before append");
      expect(Option.getOrThrow((yield* store.load(projection)).record).pending).toBeDefined();
      append = true;
      const exit = yield* leftBoundary.withTransaction(run).pipe(Effect.exit);
      expect(yield* contents).toEqual([{ id: "b", n: 2 }]);
      expect(Exit.isSuccess(exit)).toBe(true);
      const checkpoint = yield* store.load(projection);
      expect(checkpoint.token).toBe("2");
      expect(Option.getOrThrow(checkpoint.record).pending).toBeUndefined();

      yield* run;
      expect(yield* contents).toEqual([{ id: "b", n: 2 }]);
      expect(yield* store.load(projection)).toEqual(checkpoint);
    }),
);
