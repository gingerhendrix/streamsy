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
