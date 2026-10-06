import { expect, test } from "bun:test";
import { entryImports, matching } from "../../../examples/test-support/entry-imports.ts";

const dist = (name: string) => new URL(`../dist/${name}`, import.meta.url).pathname;

// `effect/sql` is allowed: it is the driverless SQL interface in `effect`.
test("the root entry reaches no SQLite driver", () => {
  expect(matching(entryImports(dist("index.js")), ["@effect/sql-sqlite-"])).toEqual([]);
});
