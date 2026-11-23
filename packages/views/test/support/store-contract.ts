import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Effect, type Scope } from "effect";
import type { Backend } from "../../src/backend.ts";
import { encodeRowKey } from "../../src/runtime/key.ts";

export function storeContract<E>(
  name: string,
  createBackend: () => Effect.Effect<Backend, E, Scope.Scope>,
): void {
  describe(name, () => {
    it.live("isolates namespaces across batched integral writes, reads and removals", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend();

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
        yield* a.remove("r", keys);
        expect(yield* a.get("r", encodeRowKey(200))).toBeUndefined();
      }),
    );

    it.live("batches arrangement changes by full entry and isolates namespaces", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend();

        const a = backend.store("a"),
          b = backend.store("b");

        const indexEntries = Array.from({ length: 201 }, (_, key) => {
          const row_key = encodeRowKey(key);

          return { row_key, index_key: "same-bucket", sort_key: row_key };
        });

        const keep = {
          row_key: encodeRowKey(0),
          index_key: "same-bucket",
          sort_key: "other-sort-key",
        };

        yield* a.indexPut("i", [...indexEntries, keep]);
        yield* b.indexPut("i", indexEntries);
        yield* a.indexRemove("i", indexEntries);
        expect(yield* a.lookup("i", "same-bucket")).toEqual([
          { row_key: keep.row_key, sort_key: keep.sort_key },
        ]);
        expect(yield* b.lookup("i", "same-bucket")).toHaveLength(201);
      }),
    );

    it.live("breaks sort-key ties by row-key code point in both directions", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend();
        const store = backend.store("a");

        const first = { row_key: encodeRowKey("zz"), sort_key: "00" },
          bmp = { row_key: encodeRowKey("\ue000\ue000"), sort_key: "11" },
          astral = { row_key: encodeRowKey("😀"), sort_key: "11" };

        yield* store.indexPut(
          "i",
          Arr.map([astral, bmp, first], (entry) => ({ ...entry, index_key: "same-bucket" })),
        );
        expect(yield* store.lookup("i", "same-bucket")).toEqual([first, bmp, astral]);
        expect(yield* store.lookup("i", "same-bucket", 2)).toEqual([first, bmp]);
        expect(yield* store.lookup("i", "same-bucket", undefined, true)).toEqual([
          astral,
          bmp,
          first,
        ]);
        expect(yield* store.lookup("i", "same-bucket", 1, true)).toEqual([astral]);
      }),
    );
  });
}
