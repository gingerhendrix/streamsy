import { describe, expect, it } from "@effect/vitest";
import { Array, Cause, Effect } from "effect";
import { Statement } from "effect/sql";
import { NonRetryable } from "@streamsy/projection";
import { encodeRowKey } from "../../src/runtime/key.ts";
import { ViewDataFault } from "../../src/sql/data-fault.ts";
import { scalarKey } from "../../src/sql/encoding.ts";
import { createBackend, serverFixtures } from "../support/backends.ts";

for (const fixture of serverFixtures) {
  describe.skipIf(fixture.server.url === undefined)(`${fixture.name} byte keys`, () => {
    it.live("round trips long keys, NUL and astral ties through truncated sort prefixes", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend(fixture);
        const store = backend.store("layout");
        const key = "x".repeat(9994);
        const encoded = encodeRowKey(key);
        expect(new TextEncoder().encode(encoded)).toHaveLength(10_000);
        const entry = { key, row: { key } };
        const withNul = { key: "x\u0000y", row: { key: "x\u0000y" } };
        const nulKey = encodeRowKey(withNul.key);
        yield* store.put("r0", [entry, withNul]);
        expect(yield* store.get("r0", encoded)).toEqual(entry);
        expect(yield* store.getMany("r0", [encoded, nulKey])).toEqual(
          new Map([
            [encoded, entry],
            [nulKey, withNul],
          ]),
        );
        yield* store.ownersPut(
          "r0",
          new Map([
            [encoded, nulKey],
            [nulKey, encoded],
          ]),
        );
        expect(yield* store.owners("r0", [encoded, nulKey])).toEqual(
          new Map([
            [encoded, nulKey],
            [nulKey, encoded],
          ]),
        );

        const sort = "ab".repeat(5000);

        const sort_key = scalarKey(sort);
        const index_key = "tied";
        const first = { row_key: nulKey, sort_key: scalarKey(sort.slice(0, -1)) };
        const bmp = { row_key: encodeRowKey("\ue000\ue000"), sort_key };
        const astral = { row_key: encodeRowKey("😀"), sort_key };
        const long = { row_key: encoded, sort_key };
        const entries = Array.map([long, astral, bmp, first], (e) => ({ ...e, index_key }));

        yield* store.indexPut("a0", entries);
        expect(yield* store.lookup("a0", index_key)).toEqual([first, bmp, astral, long]);
        expect(yield* store.lookup("a0", index_key, 2)).toEqual([first, bmp]);
        expect(yield* store.lookup("a0", index_key, 2, true)).toEqual([long, astral]);
        yield* store.indexRemove("a0", entries.slice(0, 3));
        expect(yield* store.lookup("a0", index_key)).toEqual([first]);

        const maxId = "é".repeat(127) + "x";
        const bom = "\ufeffx";
        yield* store.snapshotMark(maxId, bom);
        expect(yield* store.lookup(maxId, "")).toEqual([{ row_key: bom, sort_key: "" }]);
      }),
    );

    for (const field of ["sort_key", "row_key"] as const) {
      for (const reverse of [false, true]) {
        it.live(
          `repairs a saturated ${field} tie in ${reverse ? "reverse" : "forward"} order`,
          () =>
            Effect.gen(function* () {
              const backend = yield* createBackend(fixture);
              const store = backend.store("long-ties");
              const prefix = "x".repeat(1040);
              const index_key = "tied";

              const rows = Array.makeBy(12, (i) => {
                const suffix = i.toString().padStart(2, "0");

                return {
                  row_key: encodeRowKey(field === "row_key" ? prefix + suffix : suffix + prefix),
                  sort_key: scalarKey(field === "sort_key" ? prefix + suffix : 0),
                };
              });

              const unordered = Array.makeBy(rows.length, (i) => ({
                ...rows[(i * 7 + 5) % rows.length]!,
                index_key,
              }));

              yield* store.indexPut("a", unordered);

              const expected = (reverse ? Array.reverse(rows) : rows).slice(0, 3);
              expect(yield* store.lookup("a", index_key, 3, reverse)).toEqual(expected);
            }),
        );
      }
    }

    it.live("does no SQL or identifier validation for empty batches", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend(fixture);
        const store = backend.store("empty");
        const invalidId = "x".repeat(256);

        yield* Effect.gen(function* () {
          expect(yield* store.getMany(invalidId, [])).toEqual(new Map());
          expect(yield* store.owners(invalidId, [])).toEqual(new Map());
          yield* store.put(invalidId, []);
          yield* store.remove(invalidId, []);
          yield* store.indexPut(invalidId, []);
          yield* store.indexRemove(invalidId, []);
          yield* store.ownersPut(invalidId, new Map());
          yield* store.ownersRemove(invalidId, []);
        }).pipe(
          Effect.provideService(Statement.CurrentTransformer, () =>
            Effect.die(new Error("An empty Views batch issued SQL")),
          ),
        );
      }),
    );

    it.live("rolls back prior chunks when a concurrent batch rejects a row key", () =>
      Effect.gen(function* () {
        const backend = yield* createBackend(fixture);
        const store = backend.store("rollback");
        const lone = "\ud800";
        const first = Array.makeBy(20, (i) => ({ key: `key-${i}`, row: {} }));

        const write = store.put("rows", [
          ...first,
          { key: "sibling", row: {} },
          { key: lone, row: {} },
        ]);

        const fault = yield* backend.atomic(write).pipe(Effect.flip);
        expect(fault).toMatchObject({
          _tag: "ViewDataFault",
          view: "*",
          node: "rows",
          key: lone,
          message: '*/rows: Key "\\ud800" contains a lone surrogate',
          [NonRetryable]: true,
        });
        expect(yield* store.rows("rows")).toEqual([]);
      }),
    );

    it.live(
      "rejects ill-formed row and owner keys before persisting their replacement characters",
      () =>
        Effect.gen(function* () {
          const backend = yield* createBackend(fixture);
          const store = backend.store("layout");
          const lone = "\ud800";
          const key = encodeRowKey(lone);
          const row = { key: lone, row: {} };
          const ownership = new Map([["output", key]]);

          for (const [write, invalidKey] of [
            [store.put("r0", [row]), lone],
            [store.ownersPut("r0", ownership), key],
          ] as const) {
            const cause = yield* backend.atomic(write).pipe(Effect.sandbox, Effect.flip);
            const fault = Cause.squash(cause);
            expect(fault).toBeInstanceOf(ViewDataFault);
            expect(fault).toMatchObject({ key: invalidKey, [NonRetryable]: true });
          }

          expect(yield* store.rows("r0")).toEqual([]);
          expect(yield* store.owners("r0", ["output"])).toEqual(new Map());
          const valid = { key: "\ufffd", row: {} };
          yield* store.put("r0", [valid]);
          expect(yield* store.get("r0", encodeRowKey(valid.key))).toEqual(valid);
        }),
    );
  });
}
