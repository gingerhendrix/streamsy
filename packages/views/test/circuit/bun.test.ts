import { test, expect } from "bun:test";
import { Array, Effect } from "effect";
import { proof } from "./support/proof.ts";
import { host } from "./support/host.ts";

test("spike C circuit appends real State rows and checkpoints on Bun SQLite", async () => {
  const result = await Effect.runPromise(proof().pipe(Effect.provide(host)));
  expect(Array.map(result.units, (unit) => [unit.name, unit.outputChanges])).toEqual([
    ["load", 90],
    ["single-order", 4],
    ["customer-rename", 10],
    ["100-changes", 91],
  ]);
});
