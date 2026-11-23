/* oxlint-disable effecttsgo/strict-effect-provide -- Each test provisions its own host in a fresh database scope. */
/* oxlint-disable effecttsgo/async-function -- Bun owns the database fixtures. */
import { describe, expect, test } from "vitest";
import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/sql";
import { CommitBoundary } from "@streamsy/core";
import { OWNER_RECORDS_LIST_UNSUPPORTED, OwnerRecords, fitKey } from "@streamsy/core/internal";
import { STORAGE_MIGRATIONS, STORAGE_VERSION_TABLE_DDL } from "../../src/schema.ts";
import { storageLayer } from "../../src/storage.ts";
import {
  checkOwnerRecordPrefixes,
  explainList,
  rejectsMalformedPrefixes,
} from "../support/owner-records.ts";
import { dialectClient, freshDatabase, sqlDialects } from "./support.ts";

const dialect = sqlDialects.mysql;

const url = dialect.url;

describe.skipIf(url === undefined)("mysql owner record transactions", () => {
  test("lists a route committed after the first read of a transaction", async () => {
    if (url === undefined) return;

    await Effect.runPromise(
      Effect.gen(function* () {
        const fresh = yield* freshDatabase(dialect, url);

        const host = () =>
          Layer.build(storageLayer().pipe(Layer.provideMerge(dialectClient(dialect, fresh))));

        const left = yield* host();
        const right = yield* host();
        const first = Context.get(left, OwnerRecords);
        const second = Context.get(right, OwnerRecords);
        const leftBoundary = Context.get(left, CommitBoundary);
        const rightBoundary = Context.get(right, CommitBoundary);
        const sql = Context.get(left, SqlClient.SqlClient);

        const checkpoint = "checkpoint";
        const route = `route:${"a".repeat(550)}`;
        yield* second.write(checkpoint, "before");

        yield* leftBoundary.withTransaction(
          Effect.gen(function* () {
            const snapshot = yield* sql<{
              readonly owner_value: string;
            }>`SELECT owner_value FROM streamsy_owner_records WHERE owner_key = ${checkpoint}`;

            expect(snapshot[0]?.owner_value).toBe("before");

            yield* rightBoundary.withTransaction(
              Effect.gen(function* () {
                expect(yield* second.read(checkpoint)).toEqual(Option.some("before"));
                expect(yield* second.read(route)).toEqual(Option.none());
                yield* second.write(checkpoint, "after");
                yield* second.write(route, "late");
              }),
            );

            expect(yield* first.read(checkpoint)).toEqual(Option.some("after"));
            expect(yield* first.list("route:")).toEqual([[route, "late"]]);
          }),
        );
      }).pipe(Effect.scoped),
    );
  });
});

for (const server of [sqlDialects.pg, sqlDialects.mysql]) {
  const baseUrl = server.url;
  describe.skipIf(baseUrl === undefined)(`${server.name} owner record prefixes`, () => {
    test("lists original keys by a primary key range", async () => {
      if (baseUrl === undefined) return;

      await Effect.runPromise(
        Effect.gen(function* () {
          const fresh = yield* freshDatabase(server, baseUrl);
          const host = storageLayer().pipe(Layer.provideMerge(dialectClient(server, fresh)));

          yield* Effect.gen(function* () {
            yield* checkOwnerRecordPrefixes;
            yield* rejectsMalformedPrefixes;
            const sql = yield* SqlClient.SqlClient;

            if (server.name === "pg") {
              // Give the planner a selective predicate and current statistics.
              yield* sql`INSERT INTO streamsy_owner_records (owner_key, owner_value)
 SELECT 'filler-' || i, '' FROM generate_series(1, 2000) AS i`;
              yield* sql`ANALYZE streamsy_owner_records`;
            }

            const plan = yield* explainList('["checkpoint","a",');

            if (server.name === "pg") {
              expect(plan).toEqual(
                expect.arrayContaining([
                  {
                    "QUERY PLAN": expect.stringMatching(
                      /Index Scan using streamsy_owner_records_pkey/u,
                    ),
                  },
                  {
                    "QUERY PLAN": expect.stringMatching(/Index Cond: .*owner_key >=.*owner_key </u),
                  },
                ]),
              );
            } else {
              expect(plan).toEqual([expect.objectContaining({ type: "range", key: "PRIMARY" })]);
            }
          }).pipe(Effect.provide(host));
        }).pipe(Effect.scoped),
      );
    }, 15_000);
  });
}

const unsupported = (count: number) => ({
  operation: OWNER_RECORDS_LIST_UNSUPPORTED,
  retryable: false,
  message: expect.stringContaining(`${count} legacy hashed keys`),
});

describe.skipIf(url === undefined)("mysql legacy owner records", () => {
  test("fails closed with the hashed row count until the original keys are rewritten", async () => {
    if (url === undefined) return;

    await Effect.runPromise(
      Effect.gen(function* () {
        const fresh = yield* freshDatabase(dialect, url);
        const client = dialectClient(dialect, fresh);
        const sql = Context.get(yield* Layer.build(client), SqlClient.SqlClient);
        yield* sql.unsafe(STORAGE_VERSION_TABLE_DDL.mysql);

        for (const migration of STORAGE_MIGRATIONS.slice(0, 3))
          for (const ddl of migration.mysql) yield* sql.unsafe(ddl);
        yield* sql`INSERT INTO streamsy_schema VALUES (1, 1), (2, 2), (3, 3)`;

        const short = "short:record";
        const exact = "limit:" + "𐀀".repeat(506);
        const checkpoint = "legacy:checkpoint:" + "𐀀".repeat(520);
        const route = "legacy:route:" + "𐀀".repeat(520);

        const oldWrite = Effect.fnUntraced(function* (key: string) {
          const stored = yield* fitKey(key, 512);
          yield* sql`INSERT INTO streamsy_owner_records (owner_key, owner_value) VALUES (${stored}, 'value')`;
        });

        for (const key of [short, exact, checkpoint, route]) yield* oldWrite(key);

        const opened = yield* Layer.build(storageLayer().pipe(Layer.provide(client)));
        const records = Context.get(opened, OwnerRecords);

        expect(yield* records.list("short:")).toEqual([[short, "value"]]);
        expect(yield* records.list("limit:")).toEqual([[exact, "value"]]);
        expect(yield* records.list("legacy:").pipe(Effect.flip)).toMatchObject(unsupported(2));

        yield* records.write(checkpoint, "healed");
        expect(yield* records.list("legacy:checkpoint:")).toEqual([[checkpoint, "healed"]]);
        expect(yield* records.list("legacy:").pipe(Effect.flip)).toMatchObject(unsupported(1));
        expect(yield* records.list("legacy:route:").pipe(Effect.flip)).toMatchObject(
          unsupported(1),
        );

        const late = "late:" + "𐀀".repeat(515);
        yield* oldWrite("late-short");
        yield* oldWrite(late);
        expect(yield* records.list("late-short")).toEqual([["late-short", "value"]]);
        expect(yield* records.list("late:").pipe(Effect.flip)).toMatchObject(unsupported(1));
        // Even an unrelated prefix sharing the truncated range must fail closed.
        expect(yield* records.list(late + "extra").pipe(Effect.flip)).toMatchObject(unsupported(1));
      }).pipe(Effect.scoped),
    );
  });
});
