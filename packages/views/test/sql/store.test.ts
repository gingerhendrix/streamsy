import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array as Arr, Effect } from "effect";
import { SqlClient } from "effect/sql";
import { storeFor, migrate } from "../../src/sql/store.ts";
import { encodeRowKey } from "../../src/runtime/key.ts";

it.live(
  "migrates idempotently, isolates namespaces, and batches below SQLite's DO bind limit",
  () => {
    return Effect.gen(function* () {
      yield* migrate();
      yield* migrate();
      const sql = yield* SqlClient.SqlClient;

      const a = storeFor(sql, "a"),
        b = storeFor(sql, "b");

      const entries = Array.from({ length: 201 }, (_, key) => ({
        key,
        row: { key, text: "quote'\u0000😀" },
      }));

      yield* a.put("r", entries);
      expect(yield* a.get("r", encodeRowKey(200))).toEqual(entries[200]);
      expect(yield* b.get("r", encodeRowKey(200))).toBeUndefined();
      yield* a.remove(
        "r",
        Arr.map(entries, (e) => encodeRowKey(e.key)),
      );
      expect(yield* a.get("r", encodeRowKey(200))).toBeUndefined();
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);
