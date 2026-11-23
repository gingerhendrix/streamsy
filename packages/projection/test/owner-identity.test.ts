/* oxlint-disable effecttsgo/strict-effect-provide -- Each binding runs with the context of its own SQLite host. */
import { it } from "@effect/vitest";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { Cause, Context, Effect, Exit, Layer, Match, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { expect } from "vitest";
import { withFamilies } from "./support/families.ts";
import { Facts } from "./support/generation-outputs.ts";
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
  it.effect(`rejects a foreign projection ${operation} inside a transactional handler`, () =>
    Effect.gen(function* () {
      // Build separate Layers so the memo map allocates a separate SQLite connection for each.
      const left = yield* buildHost();
      const right = yield* buildHost();
      const leftSql = Context.get(left, SqlClient.SqlClient);
      const rightSql = Context.get(right, SqlClient.SqlClient);
      const leftStore = Context.get(left, CheckpointStore);
      const rightStore = Context.get(right, CheckpointStore);

      const foreign = Projection.process({
        ...declaration,
        id: "foreign-owner",
        commit:
          operation === "run"
            ? Commit.transaction
            : Commit.transactionWithCleanup((_, sql) =>
                sql`INSERT INTO app_rows VALUES (2)`.pipe(Effect.asVoid, Effect.orDie),
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

      const exit = yield* Projection.run(outer).pipe(Effect.provideContext(left), Effect.exit);

      expect(yield* leftSql`SELECT id FROM app_rows`).toEqual([]);
      expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([]);
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause) && Cause.squash(exit.cause)).toEqual(
        new Error("Commit boundary cannot join a transaction owned by a different SQL client"),
      );
      expect((yield* leftStore.load(outer)).token).toBe("0");
      expect(yield* rightStore.load(bound)).toEqual(before);

      yield* foreignOperation;
      expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([{ id: 2 }]);

      if (operation === "retire")
        expect(yield* rightStore.load(bound).pipe(Effect.flip)).toMatchObject({
          reason: "retired-generation",
        });
      else expect((yield* rightStore.load(bound)).token).toBe(operation === "run" ? "1" : "0");
    }),
  );
}
