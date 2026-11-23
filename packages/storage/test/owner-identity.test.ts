import { SqliteClient } from "@effect/sql-sqlite-bun";
import { it, live } from "@effect/vitest";
import { CommitBoundary, Storage, StreamId, ZERO_OFFSET, type Mutation } from "@streamsy/core";
import { CommitHooks, OwnerRecords, ProjectionMarks } from "@streamsy/core/internal";
import { SqlStorage } from "@streamsy/storage";
import { BunStorage } from "@streamsy/storage/bun";
import { Cause, Context, Effect, Exit, Layer, Option, PubSub } from "effect";
import { SqlClient, SqlError } from "effect/sql";
import { expect } from "vitest";
import { makeBoundaryTestProbe } from "../src/boundary.ts";
import { layerWithTestProbe } from "../src/storage.ts";
import { checkForeignTransaction } from "./support/foreign-sql-owner.ts";

const buildHost = () => Layer.build(BunStorage.layerProtocol({ client: { filename: ":memory:" } }));

const buildProbedHost = () => {
  const probe = makeBoundaryTestProbe();
  const client = SqlStorage.clientLayer(SqliteClient.make({ filename: ":memory:" }));

  return Layer.build(
    layerWithTestProbe({ transactionRetryAttempts: 2, transactionRetryDelayMs: 1 }, probe).pipe(
      Layer.provide(client),
    ),
  ).pipe(Effect.map((context) => ({ context, probe })));
};

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

live("a lone mutation on another client owns its retry, hooks and notifications", () =>
  Effect.gen(function* () {
    const left = yield* buildProbedHost();
    const right = yield* buildProbedHost();
    const leftStorage = Context.get(left.context, Storage);
    const rightStorage = Context.get(right.context, Storage);
    const leftFeed = yield* leftStorage.committed;
    const rightFeed = yield* rightStorage.committed;
    const leftIds: StreamId[][] = [];
    const rightIds: StreamId[][] = [];
    yield* Context.get(left.context, CommitHooks).beforeCommit((ids) =>
      Effect.sync(() => {
        leftIds.push([...ids]);
      }),
    );
    yield* Context.get(right.context, CommitHooks).beforeCommit((ids) =>
      Effect.suspend(() => {
        rightIds.push([...ids]);

        return rightIds.length === 1
          ? Effect.fail(
              new SqlError.SqlError({ reason: new SqlError.LockTimeoutError({ cause: "test" }) }),
            )
          : Effect.void;
      }),
    );
    const leftId = StreamId.make("left-rollback");
    const rightId = StreamId.make("right-retry");
    const nextRightId = StreamId.make("right-standalone");
    const leftBoundary = Context.get(left.context, CommitBoundary);

    const rolledBack = yield* leftBoundary
      .withTransaction(
        leftStorage
          .mutate(create(leftId))
          .pipe(
            Effect.andThen(rightStorage.mutate(create(rightId))),
            Effect.andThen(Effect.fail("rollback A")),
          ),
      )
      .pipe(Effect.exit);

    expect(rolledBack).toEqual(Exit.fail("rollback A"));
    expect(yield* leftStorage.record(leftId)).toEqual(Option.none());
    expect(Option.isSome(yield* rightStorage.record(rightId))).toBe(true);

    // When A commits too, it still has no ids or invalidations from B's mutation.
    yield* leftBoundary.withTransaction(rightStorage.mutate(create(nextRightId)));
    expect(left.probe).toMatchObject({ transactionAttempts: 2, invalidations: 0 });
    expect(right.probe).toMatchObject({ transactionAttempts: 3, invalidations: 2 });
    expect(leftIds).toEqual([]);
    expect(rightIds).toEqual([[rightId], [rightId], [nextRightId]]);
    expect(yield* PubSub.takeUpTo(leftFeed, Infinity)).toEqual([]);
    expect(yield* PubSub.takeUpTo(rightFeed, Infinity)).toEqual([[rightId], [nextRightId]]);
  }),
);

it.effect("a raw ambient transaction cannot use a different client's pending owner", () =>
  Effect.gen(function* () {
    const left = yield* buildHost();
    const right = yield* buildHost();
    const rightSql = Context.get(right, SqlClient.SqlClient);
    const rightStorage = Context.get(right, Storage);
    const leftFeed = yield* Context.get(left, Storage).committed;
    const rightFeed = yield* rightStorage.committed;
    const id = StreamId.make("foreign-raw");

    // Both transactions commit after the attempt, exposing any leaked write and notification.
    const exit = yield* rightSql.withTransaction(
      Context.get(left, CommitBoundary).withTransaction(
        rightStorage.mutate(create(id)).pipe(Effect.exit),
      ),
    );

    expect({
      rightRecord: yield* rightStorage.record(id),
      leftCommits: yield* PubSub.takeUpTo(leftFeed, Infinity),
      rightCommits: yield* PubSub.takeUpTo(rightFeed, Infinity),
    }).toEqual({ rightRecord: Option.none(), leftCommits: [], rightCommits: [] });
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause) && Cause.squash(exit.cause)).toEqual(
      new Error("Ambient SQL transaction is not owned by CommitBoundary"),
    );
  }),
);

it.effect("direct owner records and marks on another client survive the outer rollback", () =>
  Effect.gen(function* () {
    const left = yield* buildHost();
    const right = yield* buildHost();
    const leftRecords = Context.get(left, OwnerRecords);
    const rightRecords = Context.get(right, OwnerRecords);
    const leftMarks = Context.get(left, ProjectionMarks);
    const rightMarks = Context.get(right, ProjectionMarks);

    const exit = yield* Context.get(left, CommitBoundary)
      .withTransaction(
        Effect.gen(function* () {
          yield* leftRecords.write("checkpoint", "left");
          yield* leftMarks.mark("projection", ["binding"]);
          yield* rightRecords.write("checkpoint", "right");
          yield* rightMarks.mark("projection", ["binding"]);
          yield* rightMarks.sleep("projection", "binding", 1, 100);

          return yield* Effect.fail("rollback A");
        }),
      )
      .pipe(Effect.exit);

    expect(exit).toEqual(Exit.fail("rollback A"));
    expect(yield* leftRecords.read("checkpoint")).toEqual(Option.none());
    expect((yield* leftMarks.read("projection", ["binding"])).size).toBe(0);
    expect(yield* rightRecords.read("checkpoint")).toEqual(Option.some("right"));
    expect((yield* rightMarks.read("projection", ["binding"])).get("binding")).toMatchObject({
      seq: 1,
      wakeAt: Option.some(100),
    });
  }),
);

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
