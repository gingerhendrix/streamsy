import { CommitBoundary } from "@streamsy/core";
import { Cause, Context, Effect, Exit } from "effect";
import { SqlClient } from "effect/sql";
import { expect } from "vitest";

type Host = Context.Context<CommitBoundary | SqlClient.SqlClient>;

export const expectForeignSqlOwner = <A, E>(exit: Exit.Exit<A, E>) => {
  expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause) && Cause.squash(exit.cause)).toEqual(
    new Error("Commit boundary cannot join a transaction owned by a different SQL client"),
  );
};

export const checkForeignTransaction = (left: Host, right: Host) =>
  Effect.gen(function* () {
    const leftSql = Context.get(left, SqlClient.SqlClient);
    const rightSql = Context.get(right, SqlClient.SqlClient);
    const leftBoundary = Context.get(left, CommitBoundary);
    const rightBoundary = Context.get(right, CommitBoundary);
    yield* leftSql`CREATE TABLE app_rows (id INTEGER PRIMARY KEY)`;
    yield* rightSql`CREATE TABLE app_rows (id INTEGER PRIMARY KEY)`;

    const exit = yield* leftBoundary
      .withTransaction(
        Effect.gen(function* () {
          yield* leftSql`INSERT INTO app_rows VALUES (1)`;

          return yield* rightBoundary.withTransaction(
            Effect.gen(function* () {
              yield* rightSql`INSERT INTO app_rows VALUES (2)`;
              yield* rightSql`INSERT INTO app_rows VALUES (3)`;

              return yield* Effect.fail("after foreign SQL");
            }),
          );
        }),
      )
      .pipe(Effect.exit);

    expect(yield* leftSql`SELECT id FROM app_rows`).toEqual([]);
    expect(yield* rightSql`SELECT id FROM app_rows ORDER BY id`).toEqual([]);
    expectForeignSqlOwner(exit);
  });
