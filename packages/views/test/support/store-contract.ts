/* oxlint-disable anti-slop-effect/no-service-constructor-imports -- The suite constructs its isolated engine. */
import { describe, expect, it } from "@effect/vitest";
import { Array as Arr, Effect, type Scope } from "effect";
import type { Backend } from "../../src/backend.ts";
import { encodeRowKey } from "../../src/runtime/key.ts";
import { makeEngine, type StateChange } from "../../src/sql/engine.ts";
import { plan, ref, source } from "../sql/fixtures.ts";

const putItem = (id: string, group: string | number, n: number) => ({
  sourceId: "items",
  kind: "upsert" as const,
  key: id,
  value: { id, group, n },
});

export function storeContract<E>(
  name: string,
  use: <A, F>(
    run: (backend: Backend) => Effect.Effect<A, F>,
  ) => Effect.Effect<A, E | F, Scope.Scope>,
): void {
  describe(name, () => {
    it.live("isolates namespaces across batched integral writes, reads and removals", () =>
      use((backend) =>
        Effect.gen(function* () {
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
      ),
    );

    it.live("batches arrangement changes by full entry and isolates namespaces", () =>
      use((backend) =>
        Effect.gen(function* () {
          const a = backend.store("a"),
            b = backend.store("b");

          const indexEntries = Array.from({ length: 201 }, (_, key) => {
            const row_key = encodeRowKey(key);

            return { row_key, index_key: "same-bucket", sort_key: key.toString(16) };
          });

          const keep = {
            row_key: encodeRowKey(0),
            index_key: "same-bucket",
            sort_key: "fefe",
          };

          yield* a.indexPut("i", [...indexEntries, keep]);
          yield* b.indexPut("i", indexEntries);
          yield* a.indexRemove("i", indexEntries);
          expect(yield* a.lookup("i", "same-bucket")).toEqual([
            { row_key: keep.row_key, sort_key: keep.sort_key },
          ]);
          expect(yield* b.lookup("i", "same-bucket")).toHaveLength(201);
        }),
      ),
    );

    it.live("breaks sort-key ties by row-key code point in both directions", () =>
      use((backend) =>
        Effect.gen(function* () {
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
      ),
    );
    it.live("upserts, reads and removes accumulators and key owners by namespace", () =>
      use((backend) =>
        Effect.gen(function* () {
          const a = backend.store("a");
          const b = backend.store("b");
          const group = '["g"]';
          const initial = { count: 1, sums: { price: 2 } };
          const updated = { count: 2, sums: { price: 5 }, integerMass: { price: 5 } };
          yield* a.accumulatorPut("r", group, initial);
          yield* b.accumulatorPut("r", group, initial);
          yield* a.accumulatorPut("r", group, updated);
          expect(yield* a.accumulator("r", group)).toEqual(updated);
          expect(yield* b.accumulator("r", group)).toEqual(initial);
          yield* a.accumulatorRemove("r", group);
          expect(yield* a.accumulator("r", group)).toBeUndefined();

          const owners = new Map(
            Array.from({ length: 201 }, (_, i) => [encodeRowKey(i), encodeRowKey(`owner-${i}`)]),
          );

          const keys = [...owners.keys()];
          yield* a.ownersPut("r", owners);
          yield* b.ownersPut("r", owners);
          expect(yield* a.owners("r", [...keys, "absent"])).toEqual(owners);
          expect(yield* a.owners("other", keys)).toEqual(new Map());
          yield* a.ownersPut("r", new Map([[keys[0]!, "new-owner"]]));
          expect(yield* a.owners("r", [keys[0]!])).toEqual(new Map([[keys[0]!, "new-owner"]]));
          yield* a.ownersRemove("r", keys);
          expect(yield* a.owners("r", keys)).toEqual(new Map());
          expect(yield* b.owners("r", keys)).toEqual(owners);
        }),
      ),
    );

    it.live("marks snapshot keys idempotently and clears just the specified arrangement", () =>
      use((backend) =>
        Effect.gen(function* () {
          const a = backend.store("a");
          const b = backend.store("b");

          yield* a.snapshotMark("snapshot", "😀");
          yield* a.snapshotMark("snapshot", "😀");
          yield* a.snapshotMark("snapshot", "b");
          yield* a.snapshotMark("other", "kept");
          yield* b.snapshotMark("snapshot", "kept");
          expect(yield* a.lookup("snapshot", "")).toEqual([
            { row_key: "b", sort_key: "" },
            { row_key: "😀", sort_key: "" },
          ]);
          yield* a.clearIndex("snapshot");
          expect(yield* a.lookup("snapshot", "")).toEqual([]);
          expect(yield* a.lookup("other", "")).toEqual([{ row_key: "kept", sort_key: "" }]);
          expect(yield* b.lookup("snapshot", "")).toEqual([{ row_key: "kept", sort_key: "" }]);
        }),
      ),
    );

    it.live("loads circuit metadata, clears every table and rolls back failed atomic writes", () =>
      use((backend) =>
        Effect.gen(function* () {
          const entry = { key: "one", row: { value: 1 } };
          const key = encodeRowKey(entry.key);
          const owners = new Map([[key, "owner"]]);
          const group = '["g"]';
          const accumulator = { count: 1, sums: {} };

          const seed = (namespace: string) =>
            Effect.gen(function* () {
              const circuit = backend.circuit(namespace);
              const store = backend.store(namespace);
              expect(yield* circuit.load).toBeUndefined();
              yield* circuit.save("plan");
              yield* store.put("r", [entry]);
              yield* store.snapshotMark("i", key);
              yield* store.accumulatorPut("r", group, accumulator);
              yield* store.ownersPut("r", owners);
            });

          const read = (namespace: string) =>
            Effect.gen(function* () {
              const store = backend.store(namespace);

              return [
                yield* backend.circuit(namespace).load,
                yield* store.rows("r"),
                yield* store.lookup("i", ""),
                yield* store.accumulator("r", group),
                yield* store.owners("r", [key]),
              ];
            });

          const empty = [undefined, [], [], undefined, new Map()];

          const populated = [
            "plan",
            [entry],
            [{ row_key: key, sort_key: "" }],
            accumulator,
            owners,
          ];

          const abort = "abort unit";
          yield* backend.atomic(seed("keep"));
          expect(yield* read("keep")).toEqual(populated);
          expect(
            yield* backend
              .atomic(seed("abort").pipe(Effect.andThen(Effect.fail(abort))))
              .pipe(Effect.flip),
          ).toBe(abort);
          expect(yield* read("abort")).toEqual(empty);
          yield* backend.atomic(seed("clear"));
          yield* backend.atomic(backend.clear("clear"));
          expect(yield* read("clear")).toEqual(empty);
          expect(yield* read("keep")).toEqual(populated);
        }),
      ),
    );

    it.live("keeps group keys with NUL and lone surrogates apart", () =>
      use((backend) =>
        Effect.gen(function* () {
          const engine = yield* makeEngine({
            totals: plan("totals", [
              source("items"),
              {
                kind: "grouped-aggregate",
                id: "totals",
                input: "items",
                groupBy: { group: ref("row", "group") },
                aggregates: {
                  total: { kind: "aggregate", function: "sum", expression: ref("row", "n") },
                },
              },
            ]),
          });

          const groups = ["\u0000", "\ud800", "\udbff", "\ufffd", ""];

          const unit = (n: number) =>
            engine.process(
              backend,
              "groups",
              Arr.map(groups, (group, i) => putItem(`${n}-${i}`, group, n)),
            );

          for (const [n, total] of [
            [1, 1],
            [2, 3],
          ] as const)
            expect(Arr.map(yield* unit(n), (change) => ({ ...change }))).toEqual(
              expect.arrayContaining(
                Arr.map(groups, (group) => ({
                  view: "totals",
                  kind: "upsert",
                  key: [group],
                  value: { group, total },
                })),
              ),
            );
        }),
      ),
    );

    it.live("treats negative zero as zero in index, join and group keys", () =>
      use((backend) =>
        Effect.gen(function* () {
          const engine = yield* makeEngine({
            top: plan("top", [
              source("items"),
              {
                kind: "top-n",
                id: "top",
                input: "items",
                limit: 1,
                partitionBy: [ref("row", "group")],
                orderBy: [
                  { expression: ref("row", "n"), direction: "descending" },
                  { expression: ref("row", "id"), direction: "ascending" },
                ],
              },
            ]),
            totals: plan("totals", [
              source("items"),
              {
                kind: "grouped-aggregate",
                id: "totals",
                input: "items",
                groupBy: { group: ref("row", "group") },
                aggregates: {
                  rows: { kind: "aggregate", function: "count" },
                  largest: { kind: "aggregate", function: "max", expression: ref("row", "n") },
                },
              },
            ]),
          });

          const unit = (...changes: ReadonlyArray<StateChange>) =>
            Effect.map(engine.process(backend, "zero", changes), (emitted) =>
              Arr.map(emitted, (change) => [change.view, change.kind, encodeRowKey(change.key)]),
            );

          yield* unit(putItem("a", -0, -0), putItem("b", 0, -1));
          // The stored entries of "a" must be found again, whichever zero they were made from.
          expect(yield* unit(putItem("a", 0, 0))).toEqual([]);
          expect(yield* unit(putItem("a", -0, -2))).toEqual(
            expect.arrayContaining([
              ["top", "delete", "s1:a"],
              ["top", "upsert", "s1:b"],
              ["totals", "upsert", "a:n1:0"],
            ]),
          );
          expect(
            yield* unit(
              { sourceId: "items", kind: "delete", key: "a" },
              { sourceId: "items", kind: "delete", key: "b" },
            ),
          ).toEqual(
            expect.arrayContaining([
              ["top", "delete", "s1:b"],
              ["totals", "delete", "a:n1:0"],
            ]),
          );
          expect(yield* unit(putItem("c", 0, 0))).toEqual(
            expect.arrayContaining([["totals", "upsert", "a:n1:0"]]),
          );
        }),
      ),
    );
  });
}
