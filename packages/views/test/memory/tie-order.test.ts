/* oxlint-disable effecttsgo/strict-effect-provide -- Each test allocates an isolated memory owner. */
import { describe, expect, it } from "@effect/vitest";
import { CommitBoundary, Streamsy } from "@streamsy/core";
import { Effect } from "effect";
import type { JsonObject, RowKey } from "../../src/ir/contracts.ts";
import { makeEngine, type StateChange } from "../../src/sql/engine.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { plan, ref, source } from "../sql/fixtures.ts";
import { memoryHost } from "../support/memory.ts";

const tied = plan("top", [
  source("items"),
  {
    kind: "top-n",
    id: "top",
    input: "items",
    limit: 1,
    orderBy: [{ expression: ref("row", "rankId"), direction: "ascending" }],
  },
]);

const row = (id: string) => ({ id, rankId: 1 });

const put = (id: string) =>
  ({ sourceId: "items", kind: "upsert", key: id, value: row(id) }) as const;

const remove = (id: string) => ({ sourceId: "items", kind: "delete", key: id }) as const;

describe("Memory Views ties", () => {
  // Both keys have the same UTF-8 replacement form, and memory keeps them distinct.
  for (const [first, second] of [
    ["\ud800", "\udbff"],
    ["\udbff", "\ud800"],
  ] as const)
    for (const deleted of [first, second])
      it.live(
        `replaces a deleted top row among lone-surrogate ties: ${JSON.stringify([first, second, deleted])}`,
        () =>
          Effect.gen(function* () {
            const backend = yield* currentBackend;
            const boundary = yield* CommitBoundary;
            const engine = yield* makeEngine({ top: tied });
            const output = new Map<RowKey, JsonObject>();

            const unit = (changes: readonly StateChange[]) =>
              boundary.withTransaction(engine.process(backend, "ties", changes)).pipe(
                Effect.map((emitted) => {
                  for (const change of emitted)
                    if (change.kind === "delete") output.delete(change.key);
                    else output.set(change.key, change.value);
                }),
              );

            yield* unit([put(first)]);
            yield* unit([put(second)]);
            expect([...output.keys()]).toEqual(["\ud800"]);
            yield* unit([remove(deleted)]);
            const kept = deleted === first ? second : first;
            expect([...output]).toEqual([[kept, row(kept)]]);
          }).pipe(Effect.provide(memoryHost(Streamsy.group()))),
      );
});
