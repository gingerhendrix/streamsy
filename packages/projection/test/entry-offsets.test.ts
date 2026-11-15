import { layer } from "@effect/vitest";
import { expect, test } from "vitest";
import { Array, Effect } from "effect";
import { emptySlice } from "../src/batch.ts";
import { readSlice, reproduce } from "../src/read.ts";
import { entryOffsets } from "./support/entry-offsets.ts";
import { memoryHost } from "./support/memory-host.ts";
import { layerSqlite } from "./support/sqlite.ts";
import * as FamilyFixture from "./support/families.ts";
import { Schema } from "effect";
import { ZERO_OFFSET } from "@streamsy/core";
import { attachOffsets } from "@streamsy/core/internal";
import { Streams, StreamsReader } from "@streamsy/core/protocol";

for (const [name, host] of [
  ["memory", memoryHost({ readLimit: 3 })],
  ["SQLite", layerSqlite(3)],
] as const) {
  layer(FamilyFixture.withFamilies(host))((it) => {
    it.effect(`${name}: storage offsets survive paging and a pinned over-read replay`, () =>
      Effect.gen(function* () {
        const result = yield* entryOffsets;
        expect(result.first).toBe("Failure");
        expect(result.skippedOffsets).toEqual([[], [], []]);
        expect(result.replay).toBe(3);
        expect(result.rest).toBe(2);
        expect(result.keys[1]).toBe(result.keys[0]);
        expect(result.slices[1]).toEqual(result.slices[0]);
        expect(result.entries.slice(3, 6)).toEqual(result.entries.slice(0, 3));
        const kept = [result.slices[0]!, result.slices[2]!];
        expect(Array.flatMap(kept, (slice) => slice.offsets ?? [])).toEqual(result.expected);
        expect(
          Array.map(
            [...result.entries.slice(0, 3), ...result.entries.slice(6)],
            (entry) => entry.offset,
          ),
        ).toEqual(result.expected);

        for (const slice of result.slices) {
          expect(slice.offsets).toHaveLength(slice.items.length);
          expect(slice.offsets?.at(-1)).toBe(slice.nextOffset);

          for (const [i, offset] of (slice.offsets ?? []).entries()) {
            const after = yield* readSlice(
              "input",
              FamilyFixture.json("entry-offsets", { schema: Schema.Finite }),
              offset,
            );

            expect(after.slice.items[0]).toBe(
              (slice.items[i] ?? 0) + 10 > 50 ? undefined : (slice.items[i] ?? 0) + 10,
            );
          }
        }
      }),
    );

    it.effect(
      `${name}: missing or misaligned metadata stays unavailable without failing the read`,
      () =>
        Effect.gen(function* () {
          const input = FamilyFixture.json("offsets-metadata", { schema: Schema.Finite });
          yield* Streams.create(input);
          yield* Streams.append(input, [1, 2]);
          const reader = yield* StreamsReader;

          for (const offsets of [undefined, ["wrong-length"]]) {
            const custom = {
              ...reader,
              read: (
                id: Parameters<typeof reader.read>[0],
                options: Parameters<typeof reader.read>[1],
              ) =>
                reader
                  .read(id, options)
                  .pipe(
                    Effect.map((result) =>
                      offsets === undefined ? { ...result } : attachOffsets({ ...result }, offsets),
                    ),
                  ),
            };

            const result = yield* readSlice("input", input, ZERO_OFFSET).pipe(
              FamilyFixture.provideReader(custom),
            );

            expect(result.slice.items).toEqual([1, 2]);
            expect(result.slice).not.toHaveProperty("offsets");

            const replay = yield* reproduce("input", input, {
              from: ZERO_OFFSET,
              nextOffset: result.slice.nextOffset,
              count: 2,
            }).pipe(FamilyFixture.provideReader(custom));

            expect(replay.slice).not.toHaveProperty("offsets");
          }
        }),
    );

    it.effect(`${name}: empty reads and terminal empty pins know all their offsets`, () =>
      Effect.gen(function* () {
        const input = FamilyFixture.json("offsets-empty", { schema: Schema.Finite });
        yield* Streams.create(input);
        expect((yield* readSlice("input", input, ZERO_OFFSET)).slice.offsets).toEqual([]);
        expect(
          (yield* reproduce("input", input, {
            from: ZERO_OFFSET,
            nextOffset: ZERO_OFFSET,
            count: 0,
            closed: true,
          })).slice.offsets,
        ).toEqual([]);
      }),
    );
  });
}

test("a skipped input has offsets: []", () => {
  expect(emptySlice("cursor").offsets).toEqual([]);
});
