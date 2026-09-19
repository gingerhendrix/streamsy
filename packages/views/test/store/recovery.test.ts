import { expect, test } from "bun:test";
import { Effect, Schema } from "effect";
import type { JsonValue } from "../../src/store/contracts.ts";
import { makeMemoryBacking, memoryService } from "@streamsy/views/store";
import { recover } from "../../src/store/recovery.ts";

const encodeJsonString = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

test("recovery folds only the suffix after the latest checkpoint and is idle at the tail", () => {
  const store = memoryService(makeMemoryBacking());
  const identity = {
    planName: "recovery",
    planHash: "h",
    partition: "p",
    sourceId: "s",
    reducerId: "r",
    reducerVersion: 1,
  } as const;
  return Effect.runPromise(
    Effect.gen(function* () {
      yield* store.saveCheckpoint({
        ...identity,
        sourceCursor: "2",
        createdAtMs: 2,
        entries: [{ key: "total", value: 3 }],
      });
      const seen: string[] = [];
      const options = {
        store,
        checkpoint: identity,
        source: {
          readAfter: (cursor: string | undefined) =>
            Effect.sync(() => {
              seen.push(cursor ?? "start");
              return cursor === "2"
                ? { items: [3, 4], afterExclusiveCursor: "4" }
                : { items: [], afterExclusiveCursor: cursor };
            }),
        },
        reducer: {
          fold: (state: ReadonlyMap<string, JsonValue>, items: readonly number[]) =>
            Effect.sync(() => {
              const total =
                Number(state.get(encodeJsonString("total")) ?? 0) +
                items.reduce((a, b) => a + b, 0);
              return {
                state: new Map([["total", { key: "total", value: total }]]),
                commit: {
                  identity,
                  batchId: "recovery-4",
                  committedAtMs: 4,
                  rows: [
                    {
                      kind: "put" as const,
                      namespace: { ...identity, id: "rows" },
                      key: "total",
                      value: total,
                    },
                  ],
                  reducerStates: [
                    {
                      kind: "put" as const,
                      namespace: { ...identity, id: "r" },
                      key: "total",
                      value: total,
                    },
                  ],
                  changes: [
                    { kind: "enter" as const, relationId: "rows", key: "total", after: total },
                  ],
                },
              };
            }),
        },
      };
      const first = yield* recover(options);
      expect(first.folded).toBe(2);
      expect(seen).toEqual(["2"]);
      expect(yield* store.getRow({ ...identity, id: "rows" }, "total")).toBe(10);
      const idle = yield* recover({
        ...options,
        source: {
          readAfter: (cursor) => Effect.succeed({ items: [], afterExclusiveCursor: cursor }),
        },
      });
      expect(idle.committed).toBe(false);
      expect((yield* store.historyBounds(identity)).latest).toBe(1);
    }),
  );
});
