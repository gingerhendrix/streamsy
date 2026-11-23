import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array as Arr, Effect } from "effect";
import { SqlClient } from "effect/sql";
import { View } from "../../src/index.ts";
import { backendFor } from "../../src/sql/backend.ts";
import { encodeRowKey } from "../../src/runtime/key.ts";

it.live(
  "migrates idempotently, isolates namespaces, and batches below SQLite's DO bind limit",
  () => {
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* View.migrate(sql);
      yield* View.migrate(sql);
      const backend = yield* backendFor(sql);

      const a = backend.store("a"),
        b = backend.store("b");

      const entries = Array.from({ length: 201 }, (_, key) => ({
        key,
        row: { key, text: "quote'\u0000😀" },
      }));

      yield* a.put("r", entries);
      expect(yield* a.get("r", encodeRowKey(200))).toEqual(entries[200]);
      expect(yield* b.get("r", encodeRowKey(200))).toBeUndefined();
      const keys = Arr.map(entries, (e) => encodeRowKey(e.key));
      const all = yield* a.getMany("r", [...keys, "absent"]);
      expect(all.size).toBe(201);
      expect(all.get(encodeRowKey(200))).toEqual(entries[200]);
      expect((yield* b.getMany("r", keys)).size).toBe(0);
      yield* a.remove(
        "r",
        Arr.map(entries, (e) => encodeRowKey(e.key)),
      );
      expect(yield* a.get("r", encodeRowKey(200))).toBeUndefined();

      const indexEntries = Arr.map(keys, (row_key) => ({
        row_key,
        index_key: "same-bucket",
        sort_key: row_key,
      }));

      const keep = { row_key: keys[0]!, index_key: "same-bucket", sort_key: "other-sort-key" };
      yield* a.indexPut("i", [...indexEntries, keep]);
      yield* b.indexPut("i", indexEntries);
      yield* a.indexRemove("i", indexEntries);
      expect(yield* a.lookup("i", "same-bucket")).toEqual([
        { row_key: keep.row_key, sort_key: keep.sort_key },
      ]);
      expect(yield* b.lookup("i", "same-bucket")).toHaveLength(201);
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })));
  },
);
