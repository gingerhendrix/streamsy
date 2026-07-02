# Streamsy API

Streamsy exposes a protocol factory over a flat storage adapter.

## Core

```ts
import {
  createHttpHandler,
  createMemoryStorageAdapter,
  createStreamProtocol,
} from "@streamsy/core";

const adapter = createMemoryStorageAdapter();
const protocol = createStreamProtocol({ storage: { adapter } });
const handler = createHttpHandler({ protocol, pathPrefix: "/" });
```

`createStreamProtocol({ storage: { adapter }, clock?, longPollTimeoutMs? })` returns a protocol
factory. Optional `clock` and `longPollTimeoutMs` are provided inline on the dependency object.

On success, `create` returns the bound protocol stream directly; existing streams are resolved with
`get`:

```ts
const created = await protocol.create("/streams/a", { contentType: "text/plain" });
if (created.status === "created" || created.status === "exists") {
  await created.stream.append({ contentType: "text/plain", data: bytes });
}

const lookup = await protocol.get("/streams/a");
if (lookup.status === "ok") {
  const read = await lookup.stream.read({ offset: "-1" });
  const metadata = await lookup.stream.metadata();
}
```

`PUT` uses `protocol.create(...)`. Existing-stream HTTP methods resolve with `protocol.get(...)` first, then call the returned bound protocol stream.

## Optimistic concurrency: `expectedOffset` (Streamsy extension)

`AppendOptions.expectedOffset` is a compare-and-swap precondition: the append succeeds only if the
stream's tail offset still equals the given offset. On mismatch nothing is written (messages, close
flag, and producer state are untouched) and the append returns
`{ status: "conflict", conflictReason: "expected-offset", offset }`, where `offset` is the actual
tail. `ZERO_OFFSET` means "append only if the stream is still empty". The check is atomic with the
append because the protocol builds a declarative mutation plan and the storage adapter commits that
plan with an atomic per-stream compare-and-swap.

The intended pattern is a materialize → validate → append → retry loop:

```ts
for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
  const { state, headOffset } = await materialize(stream); // read until upToDate
  const event = buildEvent(state, input); // rebuild from fresh state each attempt
  const result = await stream.append({ ...event, expectedOffset: headOffset });
  if (result.status === "appended") return result;
  if (result.status === "conflict" && result.conflictReason === "expected-offset") continue;
  throw new Error("append failed");
}
```

Because a successful CAS proves no event landed between the read and the append, validation always
ran against exactly the state the append is conditioned on — correct across processes and storage
adapters without server-side queues.

Over HTTP the precondition is the `Stream-Expected-Offset` request header on `POST`:

- absent header: appends behave exactly as in the upstream Durable Streams protocol (the extension
  is opt-in);
- malformed offset: `400 Invalid expected offset`;
- mismatch: `409` with body `Expected offset mismatch` and the actual tail in `stream-next-offset`
  (distinguishable from the closed-stream `409`, which carries `stream-closed: true`).

Notes:

- precedence: `closed`, `content-type`, and `sequence` conflicts are reported before
  `expected-offset`;
- a close-only append on an already-closed stream remains an idempotent success and skips the
  check (nothing is written, so no update can be lost);
- `expectedOffset` is per-append: it is not meaningful with `@streamsy/json`'s `appendMany`, whose
  appends run concurrently;
- this is a Streamsy extension — the upstream Durable Streams protocol has no append precondition.

## Storage adapters

Storage packages implement one flat `StorageAdapter`. Every per-stream method takes `streamId`
first, the lifecycle intents (`create` / `fork` / `delete`) take a plan that carries the id, and
nothing lifetime-bearing or non-serializable crosses the seam (no returned per-stream handle, no
`AbortSignal`). See [`adapter-authoring.md`](./adapter-authoring.md) for the full contract.

Storage authors implement:

- **reads**: `getRecord(streamId)`, `listMessages(streamId, options?)`,
  `getProducerState(streamId, producerId)`.
- **write**: `append(streamId, AppendPlan)` applies one atomic mutation — pre-framed messages, the
  required record patch (offset/counter advance, with `lifecycle.closed` folding a close, and a
  lifecycle-only TTL renewal as the one shape that patches without advancing), an
  optional producer compare-and-set, all guarded by `preconditions` (`expectedOffset` /
  `expectedClosed` / producer CAS). It returns `appended` with the fresh record or
  `precondition-failed` with `reason` (`offset` | `closed` | `producer`) and the latest record. A
  lifecycle-only TTL "touch" is an `append` whose patch carries only `lifecycle.expiresAtMs`.
- **live wait** (required): `awaitChange(streamId, AwaitChangeOptions)` is level-triggered and fully
  serializable. A backend that can wake cheaply does so; one that cannot implements `awaitChange` by
  polling its own durable reads. Core wires in no polling fallback, but exports the
  contract-faithful loop (`runAwaitChangeLoop`) so an adapter supplies only `readRecord` +
  `waitForWake`.
- **expiry**: `scheduleExpiry(streamId, at)`, `cancelExpiry(streamId)`.
- **lifecycle**: `create(CreatePlan)`, optional `fork(ForkPlan)`, `delete(DeletePlan)` materialize
  existence and adapter-private lineage. `fork` is capability-by-presence: omit it and forks are
  `not-supported` for that backend (no core fork fallback), while every non-fork operation stays
  fully supported.

Expiry scheduling is core's responsibility: after a successful mutation core calls
`scheduleExpiry` / `cancelExpiry` back on the adapter. Plans carry no after-commit effects —
adapters only persist the plan.

Consistency boundary:

- operations on one stream are per-stream linearizable through the adapter's atomic commit point
  (SQLite transaction/CAS, Durable Object input-gate turn, or synchronous memory section);
- cross-stream operations are not globally atomic. Fork edge registration and delete/GC cascades are
  convergent, idempotent sagas. A retry or later GC/delete pass can repair a missing lineage edge or
  finish reclaiming an already-soft-deleted parent.

Protocol-bound streams are distinct from storage-bound streams:

- storage streams persist records/messages through the storage-author seam;
- protocol streams expose durable-stream operations: `append`, `read`, `readLive`, `metadata`, and `delete`.

## Packages

| Package                            | Purpose                                                                                                                         |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `@streamsy/core`                   | Protocol factory, HTTP handler, shared result/types, and the in-memory `StorageAdapter` for tests, examples, and local servers. |
| `@streamsy/json`                   | Typed JSON protocol/stream wrappers over a `StreamProtocolFactory`.                                                             |
| `@streamsy/state`                  | Durable State protocol/stream wrappers: typed change/control messages over collections.                                         |
| `@streamsy/storage-sqlite`         | Bun `bun:sqlite` `StorageAdapter` for durable local persistence.                                                                |
| `@streamsy/storage-durable-object` | Cloudflare Durable Object `StorageAdapter` and storage class.                                                                   |

## Public exports

Core exports include:

- `createStreamProtocol`, `StreamProtocol`, `createHttpHandler`, `HttpHandler`, `ZERO_OFFSET`
- protocol result/input types including `ProtocolStream`, `ProtocolGetResult`, `CreateResult`, `AppendResult`, `ReadResult`, `ReadLiveResult`, `MetadataResult`, and `DeleteResult`
- the flat storage-adapter seam: `StorageAdapter` (with the grouping facets `StreamReader`, `StreamAppender`, `StreamLiveWaiter`, `StreamExpiryScheduler`), plan types `AppendPlan`, `CreatePlan`, `ForkPlan`, `DeletePlan`, adapter result types `StorageAppendResult`, `StorageCreateResult`, `StorageForkResult`, `StorageDeleteResult`, and the live-wait types `StreamChangeSnapshot`, `AwaitChangeOptions`, `AwaitChangeResult`
- the core-internal per-stream binding for adapter authors and tests: `bindStream` and `BoundStream`
- the level-triggered `awaitChange` building blocks every adapter uses to implement its live wait (including a polling one): `runAwaitChangeLoop` (with `AwaitChangeLoopDeps`), `buildChangeSnapshot`, `changeSnapshotDiffers`, and `compareOffsets`
- the reusable adapter conformance kit: `runStorageAdapterContract` (with `MakeStorageAdapter` and `StorageAdapterContractHarness`)
- lineage strategy helpers for storage authors: `LineageStore`, `LineagePolicy`, `cascadeReclaim`, `plainPurge`, `refCountLineage`, `reverseIndexLineage`, `copyOnForkReclaim`, and `ttlOnlyReclaim`
- structured unsupported-feature helpers including `notSupported`, `isNotSupported`, `NotSupportedError`, and `unsupported`
- the in-memory storage adapter: `createMemoryStorageAdapter` and `MemoryStorageAdapterOptions`

JSON exports (`@streamsy/json`):

- `createJsonProtocol`, `JsonProtocol`, `JsonStream`, `JsonValidationError`, `normalizeJsonCodec`, `JSON_CONTENT_TYPE`
- types including `JsonCodec`, `JsonSchema`, `JsonStoredMessage`, and the typed create/get/read/readLive result and option types

State exports (`@streamsy/state`):

- `createDurableStateProtocol`, `DurableStateProtocol`, `DurableStateStream`
- types including `DurableStateCollectionDef`, `DurableStateMessage`, `ChangeMessage`, `ControlMessage`, and the typed create/get/read result and option types
- re-exports `JsonCodec` and `JsonSchema` from `@streamsy/json` for schema authoring

Durable Object exports:

- `createDurableObjectStorageAdapter` and `DurableObjectStorageAdapterOptions`
- `DurableObjectStreamStorage`
