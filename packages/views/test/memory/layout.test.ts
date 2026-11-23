/* oxlint-disable effecttsgo/strict-effect-provide -- The two fixtures run under separate owners. */
import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { layoutGroup, layoutNamespace, runLayoutUnits } from "../circuit/support/layout.ts";
import { hostFor, sqlite } from "../support/backends.ts";
import { logicalDump } from "../support/layout-dump.ts";
import { memory, memoryHost } from "../support/memory.ts";
import { registerRollback } from "../support/rollback.ts";

const fixedDump = runLayoutUnits.pipe(Effect.andThen(logicalDump(layoutNamespace)));

it.live("matches SQLite's logical dump, including full arrangement keys", () =>
  Effect.gen(function* () {
    const expected = yield* fixedDump.pipe(Effect.provide(hostFor(sqlite, layoutGroup)));
    const actual = yield* fixedDump.pipe(Effect.provide(memoryHost(layoutGroup)));
    expect(actual).toEqual(expected);
    expect(actual.arrangements.length).toBeGreaterThan(0);
  }),
);

registerRollback(memory);

registerRollback(sqlite);
