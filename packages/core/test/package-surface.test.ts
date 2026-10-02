import { expect, test } from "bun:test";
import * as Root from "@streamsy/core";
import * as Protocol from "@streamsy/core/protocol";
import * as Http from "@streamsy/core/http";
import * as InternalMemory from "@streamsy/core/internal/memory";
import manifest from "../package.json";

test("public roots expose application tools and isolate internals", () => {
  expect(Object.keys(Root.DurableState)).toEqual(["changes", "remove", "upsert"]);
  expect(Object.keys(Root)).not.toContain("MemoryCommitBoundary");
  expect(Object.keys(Protocol.Protocol)).not.toContain("create");
  expect(Object.keys(Protocol.Protocol)).not.toContain("expireIfNeeded");
  expect(Object.keys(Http)).toEqual([
    "ShutdownSignal",
    "app",
    "checkReadOptions",
    "layerShutdownSignal",
    "read",
    "routes",
    "securityHeaders",
    "streamPath",
  ]);
  expect(InternalMemory.MemoryCommitBoundary.key).toBe(
    "@streamsy/core/internal/MemoryCommitBoundary",
  );
  expect(Object.keys(manifest.exports)).toEqual([
    ".",
    "./internal/memory",
    "./testing",
    "./http",
    "./fetch",
    "./protocol",
  ]);
});
