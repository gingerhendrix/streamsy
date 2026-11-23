import { SqliteClient } from "@effect/sql-sqlite-bun";
import { it } from "@effect/vitest";
import { CommitBoundary, Storage, StreamId, ZERO_OFFSET, type Mutation } from "@streamsy/core";
import { CommitHooks } from "@streamsy/core/internal";
import { SqlStorage } from "@streamsy/storage";
import { BunStorage } from "@streamsy/storage/bun";
import { Context, Effect, Exit, Layer, Option, PubSub } from "effect";
import { SqlClient } from "effect/sql";
import { expect } from "vitest";
import { checkForeignTransaction, expectForeignSqlOwner } from "./support/foreign-sql-owner.ts";

const buildHost = () => Layer.build(BunStorage.layerProtocol({ client: { filename: ":memory:" } }));

const create = (id: StreamId): Mutation => ({
  operations: [
    {
      _tag: "Create",
      record: {
        id,
        config: { contentType: "text/plain", createdAt: 0 },
        lifecycle: { closed: false, softDeleted: false },
        currentOffset: ZERO_OFFSET,
      },
      initialMessages: [],
    },
  ],
});

it.effect("rejects a nested transaction on a separate SQLite database", () =>
  Effect.gen(function* () {
    const left = yield* buildHost();
    const right = yield* buildHost();
    yield* checkForeignTransaction(left, right);
  }),
);

for (const rawAmbient of [false, true]) {
  it.effect(`rejects a foreign Storage mutation (raw ambient transaction: ${rawAmbient})`, () =>
    Effect.gen(function* () {
      const left = yield* buildHost();
      const right = yield* buildHost();
      const rightSql = Context.get(right, SqlClient.SqlClient);
      const rightStorage = Context.get(right, Storage);
      const id = StreamId.make("foreign");

      const transaction = Context.get(left, CommitBoundary)
        .withTransaction(
          rightStorage.mutate(create(id)).pipe(Effect.andThen(Effect.fail("after mutation"))),
        )
        .pipe(Effect.exit);

      // Observe A's failure inside B's raw transaction so it would commit any leaked write.
      const exit = yield* rawAmbient ? rightSql.withTransaction(transaction) : transaction;

      expect(yield* rightStorage.record(id)).toEqual(Option.none());
      expectForeignSqlOwner(exit);
    }),
  );
}

it.effect("a later owner runs its own hooks and publishes only its own committed ids", () =>
  Effect.gen(function* () {
    const left = yield* buildHost();
    const right = yield* buildHost();
    const leftStorage = Context.get(left, Storage);
    const rightStorage = Context.get(right, Storage);
    const leftFeed = yield* leftStorage.committed;
    const rightFeed = yield* rightStorage.committed;
    const leftIds: StreamId[][] = [];
    const rightIds: StreamId[][] = [];
    yield* Context.get(left, CommitHooks).beforeCommit((ids) =>
      Effect.sync(() => {
        leftIds.push([...ids]);
      }),
    );
    yield* Context.get(right, CommitHooks).beforeCommit((ids) =>
      Effect.sync(() => {
        rightIds.push([...ids]);
      }),
    );
    const a = StreamId.make("left");
    const b = StreamId.make("right");
    const rolledBack = StreamId.make("rolled-back");
    const leftBoundary = Context.get(left, CommitBoundary);

    yield* leftBoundary.withTransaction(leftStorage.mutate(create(a)));
    yield* leftBoundary
      .withTransaction(
        leftStorage.mutate(create(rolledBack)).pipe(Effect.andThen(Effect.fail("rollback"))),
      )
      .pipe(Effect.flip);
    yield* Context.get(right, CommitBoundary).withTransaction(rightStorage.mutate(create(b)));

    expect(leftIds).toEqual([[a]]);
    expect(rightIds).toEqual([[b]]);
    expect([...(yield* PubSub.takeAll(leftFeed))]).toEqual([[a]]);
    expect([...(yield* PubSub.takeAll(rightFeed))]).toEqual([[b]]);
  }),
);

it.effect("a SQL afterCommit callback can open another owner's transaction", () =>
  Effect.gen(function* () {
    const left = yield* buildHost();
    const right = yield* buildHost();
    const leftSql = Context.get(left, SqlClient.SqlClient);
    const rightSql = Context.get(right, SqlClient.SqlClient);
    const rightStorage = Context.get(right, Storage);
    const rightBoundary = Context.get(right, CommitBoundary);
    const feed = yield* rightStorage.committed;
    const id = StreamId.make("after-commit");
    const calls: StreamId[][] = [];
    yield* Context.get(right, CommitHooks).beforeCommit((ids) =>
      Effect.sync(() => {
        calls.push([...ids]);
      }),
    );
    yield* rightSql`CREATE TABLE app_rows (id INTEGER PRIMARY KEY)`;

    const write = rightSql`INSERT INTO app_rows VALUES (1)`.pipe(
      Effect.andThen(rightStorage.mutate(create(id))),
    );

    const rolledBack = rightBoundary
      .withTransaction(write.pipe(Effect.andThen(Effect.fail("after callback write"))))
      .pipe(Effect.exit);

    const leftBoundary = Context.get(left, CommitBoundary);

    yield* leftBoundary.withTransaction(leftSql.afterCommit(Effect.asVoid(rolledBack)));
    expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([]);
    expect(yield* rightStorage.record(id)).toEqual(Option.none());

    yield* leftBoundary.withTransaction(
      leftSql.afterCommit(rightBoundary.withTransaction(write).pipe(Effect.asVoid, Effect.orDie)),
    );
    expect(yield* rightSql`SELECT id FROM app_rows`).toEqual([{ id: 1 }]);
    expect(calls).toEqual([[id]]);
    expect([...(yield* PubSub.takeAll(feed))]).toEqual([[id]]);
  }),
);

for (const withoutTransforms of [false, true]) {
  it.effect(
    `SqlStorage layers join a shared client transaction (withoutTransforms: ${withoutTransforms})`,
    () =>
      Effect.gen(function* () {
        const client = yield* Layer.build(
          SqlStorage.clientLayer(
            SqliteClient.make({ filename: ":memory:", transformResultNames: (name) => name }),
          ),
        );

        const sql = Context.get(client, SqlClient.SqlClient);
        const rightSql = withoutTransforms ? sql.withoutTransforms() : sql;

        if (withoutTransforms) expect(rightSql).not.toBe(sql);

        const left = yield* Layer.build(
          SqlStorage.layerProtocol().pipe(Layer.provide(Layer.succeedContext(client))),
        );

        const right = yield* Layer.build(
          SqlStorage.layerProtocol().pipe(
            Layer.provide(Layer.succeedContext(Context.add(client, SqlClient.SqlClient, rightSql))),
          ),
        );

        const leftBoundary = Context.get(left, CommitBoundary);
        const rightBoundary = Context.get(right, CommitBoundary);
        const rightStorage = Context.get(right, Storage);
        const id = StreamId.make("shared-client");
        expect(rightBoundary).not.toBe(leftBoundary);

        const exit = yield* leftBoundary
          .withTransaction(
            rightBoundary
              .withTransaction(rightStorage.mutate(create(id)))
              .pipe(Effect.andThen(Effect.fail("rollback shared client"))),
          )
          .pipe(Effect.exit);

        expect(exit).toEqual(Exit.fail("rollback shared client"));
        expect(yield* rightStorage.record(id)).toEqual(Option.none());
      }),
  );
}
