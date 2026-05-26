# Streamsy public API

Streamsy exposes a small public surface for building Durable Streams servers and adapters. The current stabilized surface has two application composition roots:

- `createStreamProtocol()` / `StreamProtocol` for in-process stream operations.
- `createHttpHandler()` / `HttpHandler` for serving the Durable Streams HTTP protocol.

Storage adapters are a lower-level boundary. They persist stream facts and may expose runtime capabilities such as locking, notifications, and expiry scheduling. Protocol and lifecycle policy stay in `@streamsy/core`.

This document is focused API documentation. It does not describe the full HTTP method/header matrix or curl walkthrough; those belong in an example-specific guide.

## Package boundaries

| Package | Public role | Use when |
| --- | --- | --- |
| `@streamsy/core` | Protocol facade, HTTP facade, and shared protocol/storage types. | You are building an app server, integrating Streamsy into a framework/Worker, or authoring a storage adapter. |
| `@streamsy/storage-memory` | In-memory `StreamStoreAdapter` implementation and factory. | You need local development, tests, examples, or a non-persistent server. |
| `@streamsy/storage-durable-object` | Cloudflare Durable Object storage classes. | You are wiring Streamsy into a Durable Object runtime. |
| `@streamsy/conformance-tests` | Private conformance harness. | Internal validation only; not an application API. |

Applications should usually depend on `@streamsy/core` plus one storage package. They should not instantiate internal protocol services, HTTP method services, lock providers, readers, writers, or record factories.

## Recommended memory server construction

The simplest server creates one store, one protocol, and one HTTP handler:

```ts
import { createHttpHandler, createStreamProtocol } from "@streamsy/core";
import { createMemoryStreamStore } from "@streamsy/storage-memory";

const store = createMemoryStreamStore();
const protocol = createStreamProtocol(store);
const handler = createHttpHandler({ protocol, pathPrefix: "/" });

Bun.serve({
  port: 1337,
  fetch: (request) => handler.fetch(request),
});
```

Use one shared `store`/`protocol` pair for requests that should see the same stream state. For application servers that mount Streamsy under a prefix, pass the same prefix used by your router:

```ts
const handler = createHttpHandler({ protocol, pathPrefix: "/ds" });

async function fetch(request: Request): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname.startsWith("/ds/")) return handler.fetch(request);
  return new Response("Not found", { status: 404 });
}
```

Application-specific behavior such as authentication, authorization, CORS, scope activation, and write validation should generally happen before calling `handler.fetch(request)`.

## Programmatic protocol API

`StreamProtocol` is the in-process API for durable stream operations. Prefer the factory in application code:

```ts
import { createStreamProtocol, ZERO_OFFSET } from "@streamsy/core";
import { createMemoryStreamStore } from "@streamsy/storage-memory";

const store = createMemoryStreamStore();
const protocol = createStreamProtocol(store, {
  longPollTimeoutMs: 1_500,
});

await protocol.create("demo", { contentType: "application/json" });
await protocol.append("demo", {
  data: new TextEncoder().encode(JSON.stringify({ type: "hello" })),
  contentType: "application/json",
});

const result = await protocol.read("demo", { offset: ZERO_OFFSET });
if (result.status === "ok") {
  console.log(result.messages, result.nextOffset);
}
```

Direct class construction remains supported for advanced users and tests:

```ts
import { StreamProtocol } from "@streamsy/core";

const protocol = new StreamProtocol(store);
```

### `createStreamProtocol(store, options?)`

```ts
function createStreamProtocol(
  store: StreamStoreAdapter,
  options?: StreamProtocolOptions,
): StreamProtocolInterface;
```

`StreamProtocolOptions`:

```ts
interface StreamProtocolOptions {
  clock?: Clock;
  longPollTimeoutMs?: number;
}
```

The protocol facade owns validation, offset allocation, lifecycle policy, TTL/expiry behavior, fork handling, producer idempotency, and read/live-read orchestration. Storage adapters only persist facts and provide optional runtime capabilities.

### Protocol operations

`StreamProtocolInterface` exposes:

```ts
interface StreamProtocolInterface {
  create(streamId: string, options: CreateOptions): Promise<CreateResult>;
  append(streamId: string, options: AppendOptions): Promise<AppendResult>;
  read(streamId: string, options: ReadOptions): Promise<ReadResult>;
  readLive(streamId: string, options: ReadLiveOptions): Promise<ReadLiveResult>;
  metadata(streamId: string): Promise<MetadataResult>;
  delete(streamId: string): Promise<DeleteResult>;
}
```

Key contract points:

- `create()` creates a stream, optionally with initial data, closed state, TTL/expiry, or fork metadata. Existing compatible streams return `status: "exists"`; incompatible streams return `status: "conflict"` with a `CreateConflictReason`.
- `append()` appends bytes to an existing stream. It validates content type, optional sequence constraints, optional producer idempotency metadata, and optional atomic close.
- `read()` returns stored `StoredMessage` values after the requested offset, along with `nextOffset`, `upToDate`, and closed-state metadata.
- `readLive()` performs protocol-level live reads using `mode: "long-poll" | "sse"`. It returns `status: "ok"` when messages are available and `status: "timeout"` only when no new messages arrive before timeout.
- `metadata()` returns content type, offset, TTL/expiry, and closed-state metadata for existing streams.
- `delete()` applies core lifecycle/deletion policy and reports normal absence/lifecycle states as result statuses.

Most normal absence and lifecycle outcomes are returned as union statuses such as `"not-found"`, `"gone"`, or `"conflict"` rather than thrown exceptions.

## HTTP facade API

`HttpHandler` adapts a `StreamProtocolInterface` to the standard Fetch API. Prefer the factory in application code:

```ts
import { createHttpHandler } from "@streamsy/core";

const handler = createHttpHandler({
  protocol,
  pathPrefix: "/ds",
  maxMessageSize: 1024 * 1024,
});

const response = await handler.fetch(request);
```

Direct class construction remains supported:

```ts
import { HttpHandler } from "@streamsy/core";

const handler = new HttpHandler({ protocol, pathPrefix: "/ds" });
```

### `createHttpHandler(options)`

```ts
function createHttpHandler(options: HttpHandlerOptions): HttpHandlerInterface;
```

`HttpHandlerOptions`:

```ts
interface HttpHandlerOptions {
  protocol: StreamProtocolInterface;
  pathPrefix?: string;
  maxMessageSize?: number;
}
```

`HttpHandlerInterface` is intentionally structural and minimal:

```ts
interface HttpHandlerInterface {
  fetch(request: Request): Promise<Response>;
}
```

Contract points:

- `fetch(request)` is the universal server adapter for Bun, Cloudflare Workers, and framework route handlers.
- `pathPrefix` should match where your router mounts Streamsy, for example `/` or `/ds`.
- `maxMessageSize` limits accepted create/append bodies. The default is 1 MiB.
- CORS, authentication, read-only policy, and app-specific route activation are application concerns unless a future API explicitly adds hooks.

## Exported types

Import public types from package entrypoints, not from internal source paths.

### `@streamsy/core`

Composition roots and constants:

```ts
export { createStreamProtocol, StreamProtocol, ZERO_OFFSET } from "@streamsy/core";
export { createHttpHandler, HttpHandler } from "@streamsy/core";
```

Protocol types:

```ts
export type {
  StreamProtocolInterface,
  StreamProtocolOptions,
  StreamStoreFactory,
  CreateOptions,
  CreateResult,
  CreateConflictReason,
  AppendOptions,
  AppendResult,
  ProducerOptions,
  ReadOptions,
  ReadResult,
  ReadLiveOptions,
  ReadLiveResult,
  MetadataResult,
  DeleteResult,
} from "@streamsy/core";
```

HTTP types:

```ts
export type {
  HttpHandlerInterface,
  HttpHandlerOptions,
} from "@streamsy/core";
```

Storage adapter types:

```ts
export type {
  StreamId,
  Offset,
  StreamConfig,
  StreamLifecycleState,
  StreamRecord,
  StreamRecordPatch,
  CreateStreamRecordResult,
  StreamStoreAdapter,
  StoredMessage,
  ProducerState,
  ListMessagesOptions,
  StreamEventType,
  WaitForEventOptions,
  WaitForEventResult,
  Clock,
} from "@streamsy/core";
```

### `@streamsy/storage-memory`

```ts
export { MemoryStreamStore, createMemoryStreamStore } from "@streamsy/storage-memory";

export type {
  StreamStoreAdapter,
  StreamRecord,
  StoredMessage,
  ProducerState,
} from "@streamsy/storage-memory";
```

Prefer `createMemoryStreamStore()` for ordinary construction. `MemoryStreamStore` remains exported for tests and advanced inspection.

### `@streamsy/storage-durable-object`

```ts
export {
  DurableObjectStreamStorage,
  DurableObjectStreamStoreAdapter,
} from "@streamsy/storage-durable-object";

export type {
  DurableObjectStreamStoreEnv,
  StreamStoreAdapter,
  StreamRecord,
  StoredMessage,
  ProducerState,
} from "@streamsy/storage-durable-object";
```

Durable Object applications usually need explicit Worker/Durable Object lifecycle wiring. Keep runtime-specific `fetch`, `alarm`, namespace, and environment handling in the application or storage package integration layer.

## Storage adapter boundary

`StreamStoreAdapter` is the public low-level adapter contract for persistence. It persists stream records, messages, producer state, and parent/child reference counts. Core owns durable-stream protocol semantics and lifecycle policy.

### Non-overwriting `create(record)` semantics

Adapter `create(record)` must be non-overwriting:

```ts
create(record: StreamRecord): Promise<CreateStreamRecordResult>;

type CreateStreamRecordResult =
  | { status: "created" }
  | { status: "exists"; record: StreamRecord };
```

If the stream id does not exist, persist the supplied record and return `{ status: "created" }`.

If the stream id already exists, do not overwrite it. Return `{ status: "exists", record }` with the existing record. Core compares the existing record with the requested create/fork operation and decides whether the public protocol result is `"exists"`, `"conflict"`, or another status.

This rule is important for concurrent creates, forks, and appends: storage records facts; core serializes and interprets lifecycle mutations.

### Adapter responsibilities

A storage adapter should:

1. Return `null` from `get(streamId)` when no record exists.
2. Implement `create(record)` as a non-overwriting insert.
3. Apply `update(streamId, patch)` atomically for that adapter and return the updated record.
4. Append supplied `StoredMessage` values in order without changing core-assigned offsets or timestamps.
5. Return `list(streamId, options)` results ordered by offset, exclusive of `options.after`, and respecting `until`/`limit` when provided.
6. Persist producer state facts without implementing producer idempotency policy itself.
7. Persist parent/child reference counts without implementing fork/delete lifecycle policy itself.
8. Delete stream records/messages/producer states when core requests deletion.

Optional runtime capabilities:

- `transaction(fn)` for atomic multi-record operations when supported.
- `withLock(key, fn)` for adapter/distributed locking. If omitted, core falls back to an in-process lock, which is not sufficient for multi-instance distributed storage.
- `waitForEvent()` and `notify()` for efficient live reads.
- `scheduleExpiry()` and `cancelExpiry()` for active expiry scheduling. Core decides expiry behavior and calls scheduled expiry handling.

## What remains internal

The public API is intentionally smaller than the internal implementation. Treat the following as internal and do not import them from source paths:

- protocol services such as append, create, read, live-read, fork, expiry, garbage collection, producer idempotency, record factory, message reader/writer, and lock-provider classes;
- HTTP dispatch and method services such as create, append, read, long-poll, SSE, metadata, delete, path parsing, body codecs, response factories, ETag helpers, request body readers, producer header parsers, and SSE encoders;
- conformance-test internals and test-only helpers.

Tests inside this repository may exercise internals, but applications and examples should compose through `createStreamProtocol()` and `createHttpHandler()` plus documented storage adapters/types.

## API design guardrails

- Keep `@streamsy/core` byte-stream focused. JSON, Durable State, TanStack DB, and StreamOS-specific semantics should live in examples or separate packages unless they become protocol concerns.
- Keep app policy outside the HTTP facade by default: auth, CORS, read-only routing, validation, and domain-specific writes should wrap or precede `HttpHandler.fetch()`.
- Use examples to validate API ergonomics before adding new helpers to core.
- Add new public exports deliberately at package entrypoints and document them here.
