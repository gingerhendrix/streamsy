# Streamsy API

Streamsy exposes a protocol factory over storage factories.

## Core

```ts
import { createHttpHandler, createStreamProtocol } from "@streamsy/core";
import { createMemoryStreamFactory } from "@streamsy/storage-memory";

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

Storage packages expose `StreamFactory` implementations. A storage factory returns a storage-bound `Stream` with record/message primitives and optional per-stream capabilities such as producer state, references, mutation coordination, event waiters, and expiry scheduling.

Protocol-bound streams are distinct from storage-bound streams:

- storage streams persist records/messages and optional runtime capabilities;
- protocol streams expose durable-stream operations: `append`, `read`, `readLive`, `metadata`, and `delete`.

## Packages

| Package                            | Purpose                                                           |
| ---------------------------------- | ----------------------------------------------------------------- |
| `@streamsy/core`                   | Protocol factory, HTTP handler, shared result/types.              |
| `@streamsy/storage-memory`         | In-memory `StreamFactory` for tests, examples, and local servers. |
| `@streamsy/storage-durable-object` | Cloudflare Durable Object `StreamFactory` and storage class.      |

## Public exports

Core exports include:

- `createStreamProtocol`, `StreamProtocol`, `createHttpHandler`, `HttpHandler`, `ZERO_OFFSET`
- protocol result/input types including `ProtocolStream`, `ProtocolGetResult`, `CreateResult`, `AppendResult`, `ReadResult`, `ReadLiveResult`, `MetadataResult`, and `DeleteResult`
- storage-factory types including `StreamFactory`, storage-bound `Stream`, `ComposedStreamDeps`, and optional capability interfaces
- `composeStream` and `require*` optional-capability helpers

Memory exports:

- `createMemoryStreamFactory`

Durable Object exports:

- `createDurableObjectStreamFactory`
- `DurableObjectStreamStorage`
