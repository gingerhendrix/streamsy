# Streamsy API

Streamsy exposes a protocol factory over storage factories.

## Core

```ts
import { createHttpHandler, createMemoryStreamFactory, createStreamProtocol } from "@streamsy/core";

const factory = createMemoryStreamFactory();
const protocol = createStreamProtocol({ storage: { factory } });
const handler = createHttpHandler({ protocol, pathPrefix: "/" });
```

`createStreamProtocol({ storage: { factory }, clock?, longPollTimeoutMs? })` returns a protocol
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
append because the protocol runs both inside the per-stream mutation lock.

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

## Storage factories

Storage packages expose `StreamFactory` implementations. A storage factory returns a storage-bound `Stream` with direct record/message primitives plus per-stream protocol storage methods for producer state, references, mutation coordination, event waiters, and expiry scheduling.

Protocol-bound streams are distinct from storage-bound streams:

- storage streams persist records/messages and provide direct runtime/storage methods;
- protocol streams expose durable-stream operations: `append`, `read`, `readLive`, `metadata`, and `delete`.

## Packages

| Package                            | Purpose                                                                                                                        |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `@streamsy/core`                   | Protocol factory, HTTP handler, shared result/types, and the in-memory `StreamFactory` for tests, examples, and local servers. |
| `@streamsy/json`                   | Typed JSON protocol/stream wrappers over a `StreamProtocolFactory`.                                                            |
| `@streamsy/state`                  | Durable State protocol/stream wrappers: typed change/control messages over collections.                                        |
| `@streamsy/storage-sqlite`         | Bun `bun:sqlite` `StreamFactory` for durable local persistence.                                                                |
| `@streamsy/storage-durable-object` | Cloudflare Durable Object `StreamFactory` and storage class.                                                                   |

## Public exports

Core exports include:

- `createStreamProtocol`, `StreamProtocol`, `createHttpHandler`, `HttpHandler`, `ZERO_OFFSET`
- protocol result/input types including `ProtocolStream`, `ProtocolGetResult`, `CreateResult`, `AppendResult`, `ReadResult`, `ReadLiveResult`, `MetadataResult`, and `DeleteResult`
- storage-factory types including `StreamFactory`, storage-bound `Stream`, and facet interfaces such as `StreamRecordStore`, `StreamMessageStore`, `StreamProducerStore`, `StreamReferenceTracker`, `StreamMutationCoordinator`, `StreamEventHub`, and `StreamExpiryScheduler`
- structured unsupported-feature helpers including `notSupported`, `isNotSupported`, `NotSupportedError`, and `unsupported`
- the in-memory storage adapter: `createMemoryStreamFactory` and `MemoryStreamFactoryOptions`

JSON exports (`@streamsy/json`):

- `createJsonProtocol`, `JsonProtocol`, `JsonStream`, `JsonValidationError`, `normalizeJsonCodec`, `JSON_CONTENT_TYPE`
- types including `JsonCodec`, `JsonSchema`, `JsonStoredMessage`, and the typed create/get/read/readLive result and option types

State exports (`@streamsy/state`):

- `createDurableStateProtocol`, `DurableStateProtocol`, `DurableStateStream`
- types including `DurableStateCollectionDef`, `DurableStateMessage`, `ChangeMessage`, `ControlMessage`, and the typed create/get/read result and option types
- re-exports `JsonCodec` and `JsonSchema` from `@streamsy/json` for schema authoring

Durable Object exports:

- `createDurableObjectStreamFactory`
- `DurableObjectStreamStorage`
