/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "vitest";
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
  expect(prototype.enableLoadExtension).toBeTypeOf("function");
});

test("Cloudflare entries expose wakeups and the slot layer without the raw slot tag", async () => {
  const alchemy = await import("@streamsy/serve/alchemy");
  expect(Object.keys(alchemy)).toContain("Wakeup");
  expect(Object.keys(alchemy)).toContain("Wakeups");
  expect(Object.keys(alchemy)).toContain("alarmSlotLayer");

  for (const name of ["Alarm", "AlarmSlot", "alarmLayer", "alarm", "reconcileAlarm"])
    expect(Object.keys(alchemy)).not.toContain(name);

  for (const host of ["alchemy", "cloudflare"])
    expect(
      matching(entryImports(dist(`${host}.js`)), [
        "action/runtime",
        "action/contract",
        "action/outbox",
      ]),
    ).toEqual([]);
  const runtime = await import("@streamsy/serve/action/runtime");
  expect(runtime.OutboxWakeup.make).toBeTypeOf("function");
  expect(Object.keys(runtime)).not.toContain("Wakeup");
});
