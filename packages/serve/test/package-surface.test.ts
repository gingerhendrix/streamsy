/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "bun:test";
import { Effect } from "effect";
import * as DenoHost from "@streamsy/serve/deno";
import { entryImports, matching } from "../../../examples/test-support/entry-imports.ts";

const dist = (name: string) => new URL(`../dist/${name}`, import.meta.url).pathname;

test("the root entry reaches no @effect/platform-* module", () => {
  expect(matching(entryImports(dist("index.js")), ["@effect/platform-"])).toEqual([]);
});

test("each runtime host entry reaches its own platform", () => {
  for (const host of ["bun", "node", "deno"])
    expect(entryImports(dist(`${host}.js`)).has(`@effect/platform-${host}`)).toBe(true);
});

test("the Deno SQLite helper leaves node:sqlite able to open a database", async () => {
  await Effect.runPromise(DenoHost.nodeSqliteCompat);
  await Effect.runPromise(DenoHost.nodeSqliteCompat);
  const { DatabaseSync } = await import("node:sqlite");
  const prototype: { enableLoadExtension?: unknown } = DatabaseSync.prototype;
  expect(prototype.enableLoadExtension).toBeFunction();
});
