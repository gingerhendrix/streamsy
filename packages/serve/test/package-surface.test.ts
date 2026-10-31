/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "vitest";
import { Array, Effect, Order, Record } from "effect";
import * as DenoHost from "@streamsy/serve/deno";
import manifest from "../package.json";
import { entryImports, matching } from "../../../examples/test-support/entry-imports.ts";

const dist = (name: string) => new URL(`../dist/${name}`, import.meta.url).pathname;

test("the package exposes only route, contract and host entries", () => {
  expect(Array.sort(Record.keys(manifest.exports), Order.String)).toEqual([
    ".",
    "./alchemy",
    "./bun",
    "./cloudflare",
    "./contract",
    "./deno",
    "./node",
  ]);
});

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
  expect(prototype.enableLoadExtension).toBeTypeOf("function");
});

test("Cloudflare entries expose wakeups and the slot layer without the raw slot tag", async () => {
  const alchemy = await import("@streamsy/serve/alchemy");
  expect(Record.keys(alchemy)).toContain("Wakeup");
  expect(Record.keys(alchemy)).toContain("Wakeups");
  expect(Record.keys(alchemy)).toContain("alarmSlotLayer");

  for (const name of ["Alarm", "AlarmSlot", "alarmLayer", "alarm", "reconcileAlarm"])
    expect(Record.keys(alchemy)).not.toContain(name);

  expect(alchemy.OutboxWakeup.make).toBeTypeOf("function");
});
