import { describe, expect, it } from "@effect/vitest";
import { Array, Context, Effect, Layer } from "effect";
import { SqlClient, Statement } from "effect/sql";
import { View } from "../../src/index.ts";
import { encodeRowKey } from "../../src/runtime/key.ts";
import { backendFor, client, mysql } from "../support/backends.ts";

interface Plan {
  readonly key: string | null;
  readonly type: string;
  readonly Extra: string | null;
}

describe.skipIf(mysql.server.url === undefined)("MySQL Views plans", () => {
  it.live("uses the primary key for ordered lookups, range removal, getMany and clear", () =>
    Effect.gen(function* () {
      const sql = Context.get(yield* Layer.build(client(mysql)), SqlClient.SqlClient);
      yield* View.migrate(sql);
      const backend = yield* backendFor(sql);
      const store = backend.store("target");

      const indices = Array.makeBy(256, (index) => ({
        index_key: "bucket",
        row_key: encodeRowKey(index),
        sort_key: index.toString(16).padStart(4, "0"),
      }));

      for (const namespace of ["first", "target", "last"]) {
        const rows = backend.store(namespace);
        yield* rows.put(
          "r",
          Array.makeBy(256, (key) => ({ key, row: { key } })),
        );
        yield* rows.indexPut("a", indices);
        yield* rows.accumulatorPut("n", "g", { count: 1, sums: {} });
        yield* rows.ownersPut("r", new Map([["output", "owner"]]));
        yield* backend.circuit(namespace).save("hash");
      }

      const plans = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
        Effect.gen(function* () {
          const queries: (readonly [string, readonly unknown[]])[] = [];
          yield* operation.pipe(
            Effect.provideService(Statement.CurrentTransformer, (statement) => {
              queries.push(statement.compile());

              return Effect.succeed(statement);
            }),
          );

          return yield* Effect.forEach(queries, ([query, params]) =>
            Effect.map(sql.unsafe<Plan>(`EXPLAIN ${query}`, params), (rows) => rows[0]),
          );
        });

      for (const reverse of [false, true]) {
        const [lookup] = yield* plans(store.lookup("a", "bucket", 4, reverse));
        expect(lookup).toMatchObject({ key: "PRIMARY" });
        expect(lookup?.Extra ?? "").not.toMatch(/filesort/iu);

        if (reverse) expect(lookup?.Extra).toMatch(/backward index scan/iu);
      }

      const [getMany] = yield* plans(store.getMany("r", [encodeRowKey(11), encodeRowKey(17)]));
      expect(getMany).toMatchObject({ key: "PRIMARY", type: "range" });
      const [remove] = yield* plans(store.indexRemove("a", [indices[0], indices[2]!]));
      expect(remove).toMatchObject({ key: "PRIMARY", type: "range" });
      const clear = yield* plans(backend.clear("target"));
      expect(clear).toEqual(Array.makeBy(5, () => expect.objectContaining({ key: "PRIMARY" })));
    }),
  );
});
