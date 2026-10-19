import { describe, expect, test } from "vitest";
import { Array, Order } from "effect";
import { entryImports, matching } from "../entry-imports.ts";

const fixture = (name: string) =>
  new URL(`fixtures/entry-imports/${name}`, import.meta.url).pathname;

describe("entryImports", () => {
  test("reports the bare modules of each chunk the entry reaches, and none of an unreached chunk", () => {
    const imports = entryImports(fixture("root.js"));
    expect(Array.sort(imports, Order.String)).toEqual(["chunk-bare", "lazy-bare"]);
    expect(imports.has("unreached-bare")).toBe(false);
    expect(entryImports(fixture("unreached.js"))).toEqual(new Set(["unreached-bare"]));
  });

  test("matching keeps only the names with a banned prefix", () => {
    expect(matching(entryImports(fixture("root.js")), ["lazy-", "unreached-"])).toEqual([
      "lazy-bare",
    ]);
  });
});
