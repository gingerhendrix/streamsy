import { expect, it } from "@effect/vitest";
import { Array, Effect } from "effect";
import { proof } from "./support/proof.ts";
import { host } from "./support/host.ts";

it.live("spike C circuit appends real State rows and checkpoints on Bun SQLite", () =>
  Effect.gen(function* () {
    const result = yield* proof();
    expect(Array.map(result.units, (unit) => [unit.name, unit.outputChanges])).toEqual([
      ["load", 93],
      ["single-order", 7],
      ["customer-rename", 10],
      ["100-changes", 94],
    ]);
  }).pipe(Effect.provide(host)),
);
