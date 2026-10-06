/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
/**
 * Another process writes the database outside this owner's change graph, so the
 * owner sees the write only through its bounded repair poll. This is the one
 * scenario of the removed Postgres and MySQL boundary proofs that no other
 * dialect test covers. It runs here on the product boundary.
 */
import { describe, expect, test } from "bun:test";
import { Context, Effect, Layer } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { makeBoundaryTestProbe } from "../../src/boundary.ts";
import { layerWithTestProbe } from "../../src/storage.ts";
import { runExternalRepair } from "../support/storage-boundary-scenarios.ts";
import { dialectClient, freshDatabase, sqlDialects, type SqlDialect } from "./support.ts";

const dialects: ReadonlyArray<SqlDialect> = [sqlDialects.pg, sqlDialects.mysql];

for (const dialect of dialects) {
  const url = dialect.url;
  describe.skipIf(url === undefined)(`${dialect.name} external writes`, () => {
    test("a write from another pool wakes a parked pull through bounded repair", async () => {
      if (url === undefined) return;

      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(dialect, url);
          const probe = makeBoundaryTestProbe();

          const context = yield* Layer.build(
            layerWithTestProbe({ repairIntervalMs: 100 }, probe).pipe(
              Layer.provideMerge(dialectClient(dialect, fresh)),
            ),
          );

          // A second pool with its own Reactivity stands in for another process.
          const external = Context.get(
            yield* Layer.build(dialectClient(dialect, fresh, { maxConnections: 1 })),
            SqlClient.SqlClient,
          );

          const externalWrite = external`INSERT INTO streamsy_streams ${external.insert({
            stream_id: "external",
            content_type: "text/plain",
            created_at: 0,
            current_offset: "0000000000000000_0000000000000000",
            closed: 0,
            soft_deleted: 0,
          })}`.pipe(Effect.orDie, Effect.asVoid);

          return yield* runExternalRepair(externalWrite, probe).pipe(Effect.provide(context));
        }).pipe(Effect.scoped),
      );

      expect(result).toMatchObject({ parkedAfterCommit: true, repaired: true });
      expect(result.repairPasses).toBeGreaterThan(0);
    }, 60_000);
  });
}
