import { expect, test } from "bun:test";
import * as Root from "@streamsy/core";
import * as Protocol from "@streamsy/core/protocol";
import * as Http from "@streamsy/core/http";
import * as Internal from "@streamsy/core/internal";
import manifest from "../package.json";

test("public roots expose application tools and isolate internals", () => {
  expect(Object.keys(Root.DurableState)).toEqual(["changes", "remove", "upsert"]);
  expect(Object.keys(Root)).not.toContain("OwnerRecords");
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
  expect(Internal.OwnerRecords.key).toBe("@streamsy/core/internal/OwnerRecords");
  expect(Root.CommitBoundary.key).toBe("@streamsy/core/CommitBoundary");
  expect(Object.keys(manifest.exports)).toEqual([
    ".",
    "./internal",
    "./testing",
    "./http",
    "./fetch",
    "./protocol",
  ]);
});
