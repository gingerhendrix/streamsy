/* oxlint-disable effecttsgo/strict-effect-provide -- Each test allocates an isolated memory owner. */
import { describe, expect, it } from "@effect/vitest";
import { CommitBoundary, Streamsy } from "@streamsy/core";
import { Array as Arr, Cause, Effect, Exit, Fiber } from "effect";
import { compareRowKeys, encodeRowKey } from "../../src/runtime/key.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { inBackend, memoryHost } from "../support/memory.ts";
import { storeContract } from "../support/store-contract.ts";

const host = memoryHost(Streamsy.group());

const indexEntry = (row_key: string) => ({ row_key, index_key: "bucket", sort_key: "1" });

describe("Memory Views store", () => {
  storeContract("Store", (run) => inBackend(run).pipe(Effect.provide(host)));

  it.live("separates distinct lone surrogates whose row keys compare equal", () =>
    inBackend((backend) =>
      Effect.gen(function* () {
        const store = backend.store("board");
        const first = encodeRowKey("\ud800");
        const second = encodeRowKey("\ud801");
        const absent = encodeRowKey("\ud802");

        expect(compareRowKeys(first, second)).toBe(0);
        yield* store.put("rows", [
          { key: "\ud800", row: { value: 1 } },
          { key: "\ud801", row: { value: 2 } },
        ]);
        yield* store.indexPut("i", [indexEntry(first), indexEntry(second), indexEntry(first)]);
        yield* store.indexRemove("i", [indexEntry(absent)]);
        expect(
          new Set(Arr.map(yield* store.lookup("i", "bucket"), (entry) => entry.row_key)),
        ).toEqual(new Set([first, second]));
        expect(yield* store.lookup("i", "bucket")).toHaveLength(2);
        yield* store.indexRemove("i", [indexEntry(first)]);
        yield* store.remove("rows", [first]);
        expect(yield* store.lookup("i", "bucket")).toEqual([{ row_key: second, sort_key: "1" }]);
        expect(yield* store.get("rows", first)).toBeUndefined();
        expect((yield* store.get("rows", second))?.row).toEqual({ value: 2 });
      }),
    ).pipe(Effect.provide(host)),
  );

  it.live("rolls back nested journal marks and preserves earlier and later writes", () =>
    Effect.gen(function* () {
      const backend = yield* currentBackend;
      const boundary = yield* CommitBoundary;
      const store = backend.store("book");
      const entry = { key: "old", row: { value: 1 } };
      const index = { index_key: "bucket", sort_key: "score", row_key: encodeRowKey(entry.key) };
      const next = { ...index, row_key: encodeRowKey("kept") };
      const aborted = "abort a savepoint";

      yield* boundary.withTransaction(
        Effect.gen(function* () {
          yield* store.put("rows", [entry]);
          yield* store.indexPut("i", [index]);
          yield* backend.atomic(
            Effect.gen(function* () {
              yield* store.indexPut("i", [next]);

              const nested = backend.atomic(
                Effect.gen(function* () {
                  yield* store.indexRemove("i", [index, next]);
                  yield* store.indexPut("i", [{ ...index, row_key: "temporary" }]);
                  yield* store.clearIndex("i");
                  yield* backend.clear("book");
                  yield* store.put("rows", [{ key: "bad", row: {} }]);

                  return yield* Effect.fail(aborted);
                }),
              );

              expect(yield* nested.pipe(Effect.flip)).toBe(aborted);
              expect(yield* store.rows("rows")).toEqual([entry]);
              expect(yield* store.lookup("i", "bucket")).toHaveLength(2);
            }),
          );

          const outer = backend.atomic(
            Effect.gen(function* () {
              yield* backend.atomic(store.indexRemove("i", [index, next]));

              return yield* Effect.fail(aborted);
            }),
          );

          expect(yield* outer.pipe(Effect.flip)).toBe(aborted);
          yield* store.put("rows", [{ key: "later", row: {} }]);
        }),
      );

      yield* boundary.withTransaction(
        Effect.gen(function* () {
          expect(yield* store.get("rows", encodeRowKey("later"))).toEqual({
            key: "later",
            row: {},
          });
          expect(yield* store.lookup("i", "bucket")).toEqual([
            { row_key: index.row_key, sort_key: index.sort_key },
            { row_key: next.row_key, sort_key: next.sort_key },
          ]);
        }),
      );
    }).pipe(Effect.provide(host)),
  );

  it.live("checks the owner fiber even after the backend is enlisted", () =>
    inBackend((backend) =>
      Effect.gen(function* () {
        const store = backend.store("board");
        yield* store.put("rows", [{ key: "parent", row: {} }]);
        const child = yield* store.put("rows", [{ key: "child", row: {} }]).pipe(Effect.forkScoped);
        const exit = yield* Fiber.await(child);

        expect(Exit.isFailure(exit) && Cause.findErrorOption(exit.cause)).toMatchObject({
          _tag: "Some",
          value: {
            _tag: "MemoryEnlistFault",
            message: "Enlisting requires the active memory owner fiber",
          },
        });
        expect(yield* store.get("rows", encodeRowKey("child"))).toBeUndefined();
      }),
    ).pipe(Effect.provide(host)),
  );
});
