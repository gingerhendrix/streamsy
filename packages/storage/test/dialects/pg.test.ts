import { CommitBoundary, Operations, Storage, StreamId, ZERO_OFFSET } from "@streamsy/core";
import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/sql";
import { expect, test } from "vitest";
import { storageLayer } from "../../src/storage.ts";
import { storageDialectSuite } from "./storage-suite.ts";
import { dialectClient, freshDatabase, sqlDialects } from "./support.ts";

storageDialectSuite(sqlDialects.pg);

const dialect = sqlDialects.pg;

const create = (id: StreamId) =>
  Operations.Create({
    record: {
      id,
      config: { contentType: "text/plain", createdAt: 0 },
      lifecycle: { closed: false, softDeleted: false },
      currentOffset: ZERO_OFFSET,
    },
    initialMessages: [],
  });

test.skipIf(dialect.url === undefined)(
  "a mutation with only an owned pending entry rolls back in its own Postgres transaction",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        if (dialect.url === undefined) return;
        const url = yield* freshDatabase(dialect, dialect.url);

        const host = yield* Layer.build(
          storageLayer().pipe(
            Layer.provideMerge(dialectClient(dialect, url, { maxConnections: 2 })),
          ),
        );

        const sql = Context.get(host, SqlClient.SqlClient);
        const storage = Context.get(host, Storage);
        const boundary = Context.get(host, CommitBoundary);
        const first = StreamId.make("first");
        const rejected = StreamId.make("rejected");
        yield* sql`CREATE TABLE outer_rows (id INTEGER PRIMARY KEY)`;
        yield* sql`ALTER TABLE streamsy_streams ADD CONSTRAINT reject_second CHECK (stream_id <> 'rejected')`;

        const outerFailure = yield* boundary
          .withTransaction(
            Effect.gen(function* () {
              yield* sql`INSERT INTO outer_rows VALUES (1)`;

              const error = yield* storage
                .mutate({ operations: [create(first), create(rejected)] })
                .pipe(
                  Effect.updateContext((context: Context.Context<never>) =>
                    Context.omit(sql.transactionService)(context),
                  ),
                  Effect.flip,
                );

              expect(error).toMatchObject({ _tag: "StorageFault" });

              return yield* Effect.fail("rollback outer");
            }),
          )
          .pipe(Effect.flip);

        expect(outerFailure).toBe("rollback outer");
        expect(yield* sql`SELECT id FROM outer_rows`).toEqual([]);
        expect(yield* storage.record(first)).toEqual(Option.none());
        expect(yield* storage.record(rejected)).toEqual(Option.none());
      }).pipe(Effect.scoped),
    ),
);
