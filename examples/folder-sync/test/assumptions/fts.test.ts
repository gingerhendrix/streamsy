import { expect, layer } from "@streamsy/effect-test";
import { CheckpointStore, Commit, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect, Option, Result, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Files, host } from "./fixtures.ts";

const search = Projection.process({
  id: "assumption-fts",
  params: { folderId: Schema.String },
  inputs: { input: Files },
  commit: Commit.transaction,
  process: Projection.each(({ item }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO search (text) VALUES (${item})`;
    }),
  ),
}).at({ folderId: "a" });

layer(host(":memory:"))((it) => {
  it.effect("Bun FTS rows and projection offsets commit and roll back together", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe("CREATE VIRTUAL TABLE search USING fts5(text)");
      const files = yield* Files;
      yield* files.create({ folderId: "a" });
      yield* files.append({ folderId: "a" }, ["durable filesystem", "folder history"]);
      const store = yield* CheckpointStore;

      const failed = yield* Projection.run(search).pipe(
        Effect.provideService(CheckpointStore, {
          ...store,
          compareAndSet: () =>
            Effect.fail(
              new ProjectionFault({
                phase: "checkpoint",
                reason: "storage-failure",
                message: "rollback",
              }),
            ),
        }),
        Effect.result,
      );

      expect(Result.isFailure(failed)).toBe(true);
      expect(yield* sql`SELECT * FROM search`).toEqual([]);
      expect(Option.isNone((yield* store.load(Projection.key(search))).record)).toBe(true);
      yield* Projection.run(search);
      expect(yield* sql`SELECT text FROM search WHERE search MATCH ${"durable"}`).toEqual([
        { text: "durable filesystem" },
      ]);
      expect((yield* Projection.read(search)).offsets.input).toBe(
        (yield* files.head({ folderId: "a" })).nextOffset,
      );
      yield* Projection.run(search);
      expect(yield* sql`SELECT count(*) AS n FROM search`).toEqual([{ n: 2 }]);
    }),
  );
});
