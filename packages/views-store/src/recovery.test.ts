import { expect, test } from "bun:test";
import { Effect } from "effect";
import type { JsonValue } from "./contracts.ts";
import { makeMemoryBacking, memoryService } from "./memory.ts";
import { recover } from "./recovery.ts";

test("recovery folds only the suffix after the latest checkpoint and is idle at the tail", async () => {
  const store = memoryService(makeMemoryBacking());
  const identity = {
    planName: "recovery",
    planHash: "h",
    partition: "p",
    sourceId: "s",
    reducerId: "r",
    reducerVersion: 1,
  } as const;
  await Effect.runPromise(
    store.saveCheckpoint({
      ...identity,
      sourceCursor: "2",
      createdAtMs: 2,
      entries: [{ key: "total", value: 3 }],
    }),
  );
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
            Number(state.get(JSON.stringify("total")) ?? 0) + items.reduce((a, b) => a + b, 0);
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
              changes: [{ kind: "enter" as const, relationId: "rows", key: "total", after: total }],
            },
          };
        }),
    },
  };
  const first = await Effect.runPromise(recover(options));
  expect(first.folded).toBe(2);
  expect(seen).toEqual(["2"]);
  expect(await Effect.runPromise(store.getRow({ ...identity, id: "rows" }, "total"))).toBe(10);
  const idle = await Effect.runPromise(
    recover({
      ...options,
      source: {
        readAfter: (cursor) => Effect.succeed({ items: [], afterExclusiveCursor: cursor }),
      },
    }),
  );
  expect(idle.committed).toBe(false);
  expect((await Effect.runPromise(store.historyBounds(identity))).latest).toBe(1);
});
