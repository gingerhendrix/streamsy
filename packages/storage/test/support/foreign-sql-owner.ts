import { CommitBoundary } from "@streamsy/core";
import { Context, Effect, Exit } from "effect";
import { SqlClient } from "effect/sql";
import { expect } from "vitest";

type Host = Context.Context<CommitBoundary | SqlClient.SqlClient>;

export const checkForeignTransaction = (left: Host, right: Host) =>
  Effect.gen(function* () {
    const leftSql = Context.get(left, SqlClient.SqlClient);
    const rightSql = Context.get(right, SqlClient.SqlClient);
    const leftBoundary = Context.get(left, CommitBoundary);
    const rightBoundary = Context.get(right, CommitBoundary);
    yield* leftSql`CREATE TABLE app_rows (id INTEGER PRIMARY KEY)`;
    yield* rightSql`CREATE TABLE app_rows (id INTEGER PRIMARY KEY)`;

    const failedRight = (id: number) =>
      rightBoundary.withTransaction(
        Effect.gen(function* () {
          yield* rightSql`INSERT INTO app_rows VALUES (${id})`;
          yield* rightSql`INSERT INTO app_rows VALUES (${id + 1})`;

          return yield* Effect.fail("after inner SQL");
        }),
      );

    yield* leftBoundary.withTransaction(
      Effect.gen(function* () {
        yield* leftSql`INSERT INTO app_rows VALUES (1)`;
        expect(yield* failedRight(1).pipe(Effect.exit)).toEqual(Exit.fail("after inner SQL"));
        expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([]);
      }),
    );

    const failedOuter = yield* leftBoundary
      .withTransaction(
        Effect.gen(function* () {
          yield* leftSql`INSERT INTO app_rows VALUES (2)`;
          yield* rightBoundary.withTransaction(rightSql`INSERT INTO app_rows VALUES (3)`);

          return yield* Effect.fail("after outer SQL");
        }),
      )
      .pipe(Effect.exit);

    expect(failedOuter).toEqual(Exit.fail("after outer SQL"));

    const failedBoth = yield* leftBoundary
      .withTransaction(
        leftSql`INSERT INTO app_rows VALUES (4)`.pipe(Effect.andThen(failedRight(4))),
      )
      .pipe(Effect.exit);

    expect(failedBoth).toEqual(Exit.fail("after inner SQL"));
    expect(yield* leftSql`SELECT id FROM app_rows ORDER BY id`).toEqual([{ id: 1 }]);
    expect(yield* rightSql`SELECT id FROM app_rows ORDER BY id`).toEqual([{ id: 3 }]);
  });
