import { expect, test } from "vitest";
import { entryImports, matching } from "../../../examples/test-support/entry-imports.ts";

const dist = (name: string) => new URL(`../dist/${name}`, import.meta.url).pathname;

const drivers = [
  "@effect/sql-sqlite-",
  "@effect/sql-pg",
  "@effect/sql-mysql2",
  "@effect/platform-",
];

test("the root entry reaches no SQL driver or runtime platform", () => {
  expect(matching(entryImports(dist("index.js")), drivers)).toEqual([]);
});

test("each host entry reaches its own driver", () => {
  const bun = entryImports(dist("bun.js"));
  expect(bun.has("@effect/sql-sqlite-bun")).toBe(true);
  expect(matching(bun, ["@effect/platform-bun"])).not.toEqual([]);
  expect(entryImports(dist("durable-object.js")).has("@effect/sql-sqlite-do")).toBe(true);
});
