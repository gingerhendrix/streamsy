import { expect, test } from "bun:test";
import * as StoragePackage from "../src/index.ts";

test("the public root keeps migration internals private", () => {
  expect(Object.keys(StoragePackage).toSorted()).toEqual([
    "CommitBoundary",
    "DEFAULT_REPAIR_INTERVAL_MS",
    "layer",
  ]);
});
