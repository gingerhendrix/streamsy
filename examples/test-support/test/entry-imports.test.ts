import { describe, expect, test } from "bun:test";
import { entryImports, matching } from "../entry-imports.ts";

const fixture = (name: string) =>
  new URL(`fixtures/entry-imports/${name}`, import.meta.url).pathname;

describe("entryImports", () => {
  test("reports the bare modules of each chunk the entry reaches, and none of an unreached chunk", () => {
    const imports = entryImports(fixture("root.js"));
    expect([...imports].toSorted()).toEqual(["chunk-bare", "lazy-bare"]);
    expect(imports.has("unreached-bare")).toBe(false);
    expect(entryImports(fixture("unreached.js"))).toEqual(new Set(["unreached-bare"]));
  });

  test("matching keeps only the names with a banned prefix", () => {
    expect(matching(entryImports(fixture("root.js")), ["lazy-", "unreached-"])).toEqual([
      "lazy-bare",
    ]);
  });
});
