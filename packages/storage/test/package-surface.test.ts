/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "bun:test";
import * as StoragePackage from "@streamsy/storage";
import * as BunStorage from "@streamsy/storage/bun";
import * as DurableObjectStorage from "@streamsy/storage/durable-object";

test("the public root keeps migration internals private", () => {
  expect(Object.keys(StoragePackage).toSorted()).toEqual([
    "CommitBoundary",
    "DEFAULT_REPAIR_INTERVAL_MS",
    "DEFAULT_TRANSACTION_RETRY_ATTEMPTS",
    "DEFAULT_TRANSACTION_RETRY_DELAY_MS",
    "layer",
    "layerProtocol",
  ]);
});

test("host entries and optional peers keep the package graph exact", async () => {
  expect(Object.keys(BunStorage).toSorted()).toEqual(["layer", "layerProtocol"]);
  expect(Object.keys(DurableObjectStorage).toSorted()).toEqual([
    "DEFAULT_LONG_POLL_TIMEOUT_MS",
    "layer",
    "layerProtocol",
  ]);
  const root = await Bun.file(new URL("../src/index.ts", import.meta.url)).text();
  expect(root).not.toContain("@effect/sql-sqlite-bun");
  expect(root).not.toContain("@effect/sql-sqlite-do");
  expect(root).not.toContain("@effect/platform-bun");
  const durableObject = await Bun.file(new URL("../src/durable-object.ts", import.meta.url)).text();
  expect(durableObject).not.toContain("@effect/platform-bun");
  const manifest: unknown = await Bun.file(new URL("../package.json", import.meta.url)).json();
  expect(manifest).toMatchObject({
    dependencies: { "@streamsy/core": "workspace:*", effect: "4.0.0-rc.115" },
    peerDependencies: {
      "@effect/platform-bun": "4.0.0-rc.115",
      "@effect/sql-sqlite-bun": "4.0.0-rc.115",
      "@effect/sql-sqlite-do": "4.0.0-rc.115",
    },
    peerDependenciesMeta: {
      "@effect/platform-bun": { optional: true },
      "@effect/sql-sqlite-bun": { optional: true },
      "@effect/sql-sqlite-do": { optional: true },
    },
  });
});
