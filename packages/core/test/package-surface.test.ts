import { expect, test } from "bun:test";
import * as Root from "@streamsy/core";
import * as Protocol from "@streamsy/core/protocol";
import * as Fetch from "@streamsy/core/fetch";
import * as Http from "@streamsy/core/http";
import * as InternalMemory from "@streamsy/core/internal/memory";
import manifest from "../package.json";

test("public roots expose application tools and isolate internals", () => {
  expect(Object.keys(Root.State)).toEqual(["changes", "delete", "upsert"]);
  expect(Object.keys(Root)).not.toContain("MemoryCommitBoundary");
  expect(Object.keys(Protocol.Protocol)).not.toContain("create");
  expect(Object.keys(Protocol.Protocol)).not.toContain("expireIfNeeded");
  expect(Object.keys(Http)).toEqual([
    "app",
    "checkReadOptions",
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

test("root exports stay explicit", () => {
  expect(Object.keys(Root).sort()).toEqual([
    "AnyStream",
    "AppendConflict",
    "ChangeSnapshot",
    "Changes",
    "CreateConflict",
    "DecodeFault",
    "EncodeFault",
    "Fold",
    "ForkSourceNotFound",
    "Http",
    "InvalidAppendRequest",
    "InvalidEpochSeq",
    "InvalidForkRequest",
    "InvalidReadRequest",
    "Memory",
    "MessageWindow",
    "Mutation",
    "MutationApplied",
    "MutationRejected",
    "NotSupported",
    "Offset",
    "OffsetMismatch",
    "Operation",
    "OperationResult",
    "Producer",
    "ProducerGap",
    "ProducerId",
    "ProducerPrecondition",
    "ProducerState",
    "RecordPatch",
    "StaleEpoch",
    "State",
    "Storage",
    "StorageCapabilities",
    "StorageFault",
    "StoredMessage",
    "StreamBusy",
    "StreamClosed",
    "StreamConfig",
    "StreamFamily",
    "StreamGone",
    "StreamGroup",
    "StreamId",
    "StreamLifecycle",
    "StreamNotFound",
    "StreamRecord",
    "StreamRef",
    "TransportFault",
    "ZERO_OFFSET",
    "compare",
    "isProtocolError",
    "provides",
  ]);
  expect(Object.keys(Protocol.StreamRoute).sort()).toEqual(["bytes", "custom", "json", "state"]);
});

test("family namespace pins factories and graph composition helpers", () => {
  expect(Object.keys(Root.StreamFamily).sort()).toEqual([
    "AnyStream",
    "bytes",
    "checkFamilies",
    "custom",
    "json",
    "provides",
    "state",
  ]);
});

test("protocol subpath exposes protocol services and tools", () => {
  expect(Object.keys(Protocol).sort()).toEqual([
    "Protocol",
    "StreamRoute",
    "Streams",
    "StreamsReader",
    "StreamsWriter",
    "routes",
  ]);
  expect(Object.keys(Protocol.Streams).sort()).toEqual([
    "append",
    "create",
    "follow",
    "head",
    "items",
    "layerMemory",
    "read",
    "remove",
    "session",
  ]);
});

test("Fetch exposes separate family and protocol layer constructors", () => {
  expect(Object.keys(Fetch).sort()).toEqual(["TransportFault", "layer", "layerProtocol"]);
});
