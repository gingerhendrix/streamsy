import { expect, test } from "bun:test";
import * as Root from "@streamsy/core";
import * as Protocol from "@streamsy/core/protocol";
import * as Fetch from "@streamsy/core/fetch";
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

test("root exports stay explicit", () => {
  expect(Object.keys(Root).sort()).toEqual([
    "AppendConflict",
    "ChangeSnapshot",
    "CommitBoundary",
    "CreateConflict",
    "DecodeFault",
    "DurableState",
    "EncodeFault",
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
    "Mutations",
    "NotSupported",
    "Offset",
    "OffsetMismatch",
    "Operation",
    "OperationResult",
    "OperationResults",
    "Operations",
    "Producer",
    "ProducerGap",
    "ProducerId",
    "ProducerPrecondition",
    "ProducerState",
    "RecordPatch",
    "StaleEpoch",
    "Storage",
    "StorageCapabilities",
    "StorageFault",
    "StoredMessage",
    "StreamAddress",
    "StreamBusy",
    "StreamClosed",
    "StreamConfig",
    "StreamGone",
    "StreamId",
    "StreamLifecycle",
    "StreamNotFound",
    "StreamRecord",
    "Streamsy",
    "TransportFault",
    "ZERO_OFFSET",
    "compare",
    "isProtocolError",
  ]);
  expect(Object.keys(Protocol.StreamRoute).sort()).toEqual(["bytes", "custom", "json", "state"]);
});

test("stream namespace pins factories and graph composition helpers", () => {
  expect(Object.keys(Root.Streamsy).sort()).toEqual([
    "ChangeFeed",
    "Wildcard",
    "bytes",
    "custom",
    "group",
    "json",
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
    "collect",
    "create",
    "decide",
    "fold",
    "follow",
    "head",
    "items",
    "layerMemory",
    "read",
    "remove",
    "session",
  ]);
});

test("Fetch exposes separate stream and protocol layer constructors", () => {
  expect(Object.keys(Fetch).sort()).toEqual(["TransportFault", "layer", "layerProtocol"]);
});
