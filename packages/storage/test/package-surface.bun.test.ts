/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "bun:test";
import * as StoragePackage from "../src/index.ts";
import * as BunStorage from "../src/bun.ts";
import * as DurableObjectStorage from "../src/durable-object.ts";

test("the public root keeps migration internals private", () => {
  expect(Object.keys(StoragePackage).toSorted()).toEqual([
    "CommitBoundary",
    "DEFAULT_REPAIR_INTERVAL_MS",
    "DEFAULT_TRANSACTION_RETRY_ATTEMPTS",
    "DEFAULT_TRANSACTION_RETRY_DELAY_MS",
    "layer",
  ]);
});

test("host entries and optional peers keep the package graph exact", async () => {
  expect(Object.keys(BunStorage).toSorted()).toEqual(["layer", "layerProtocol"]);
  expect(Object.keys(DurableObjectStorage)).toEqual(["layer"]);
  const root = await Bun.file(new URL("../src/index.ts", import.meta.url)).text();
  expect(root).not.toContain("@effect/sql-sqlite-bun");
  expect(root).not.toContain("@effect/sql-sqlite-do");
  const manifest: unknown = await Bun.file(new URL("../package.json", import.meta.url)).json();
  expect(manifest).toMatchObject({
    dependencies: { "@streamsy/core": "workspace:*", effect: "4.0.0-rc.112" },
    peerDependencies: {
      "@effect/sql-sqlite-bun": "4.0.0-rc.112",
      "@effect/sql-sqlite-do": "4.0.0-rc.112",
    },
    peerDependenciesMeta: {
      "@effect/sql-sqlite-bun": { optional: true },
      "@effect/sql-sqlite-do": { optional: true },
    },
  });
});
