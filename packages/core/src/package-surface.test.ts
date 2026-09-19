import { expect, test } from "bun:test";
import * as Root from "./index.ts";
import * as Http from "./http/index.ts";
import * as InternalMemory from "./internal/memory.ts";
import manifest from "../package.json";

test("public roots omit internal composition and obsolete aliases", () => {
  expect(Object.keys(Root)).not.toContain("MemoryCommitBoundary");
  expect(Object.keys(Root.Protocol)).not.toContain("create");
  expect(Object.keys(Root.Protocol)).not.toContain("expireIfNeeded");
  expect(Object.keys(Http)).toEqual(["app", "makeEdge", "streamPath"]);
  expect(InternalMemory.MemoryCommitBoundary.key).toBe(
    "@streamsy/core/internal/MemoryCommitBoundary",
  );
  expect(Object.keys(manifest.exports)).toEqual([
    ".",
    "./internal/memory",
    "./testing",
    "./http",
    "./fetch",
  ]);
});
