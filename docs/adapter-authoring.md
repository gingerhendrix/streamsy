# Authoring a Streamsy storage adapter

This guide explains the factory/composed-stream model that Streamsy storage adapters target. It is the primary reference for adapter authors. It complements `docs/api.md`, which describes the public application API surface.

The factory/composed-stream model is being introduced incrementally. The legacy `StreamStoreAdapter` interface remains supported and is the input to the compatibility composer described below. New adapters should still implement `StreamStoreAdapter` for now; the factory seam is the destination architecture, exposed alongside the existing API so adapters and downstream code can migrate without one large rewrite.

## Core model

### Factory owns lookup, routing, and composition

A factory is the architectural boundary. It maps a public stream id to an operable `Stream` and composes that stream from backend-specific dependencies.

```ts
import type { StreamFactory } from "@streamsy/core";

const factory: StreamFactory = {
  getStream(streamId) {
    // return a Stream bound to streamId
  },
};
```

Factory-owned concerns include:

- public stream id lookup;
- object, database, or path routing;
- local stream id binding;
- tenant or domain partitioning;
- composing a concrete `Stream` from storage and runtime dependencies;
- choosing canonical write paths versus derived read paths;
- cross-stream fork or reference policy;
- cross-stream expiry scans and indexes;
- shared locks or transactions when they span more than one stream.

Adapter authors should not surface placement decisions through public APIs. Placement exists as a factory implementation detail.

### Stream is bound to one stream and implements the operations directly

A returned `Stream` represents one stream and exposes the operations the protocol uses for that one stream:

```ts
import type { Stream } from "@streamsy/core";

const stream: Stream = await factory.getStream("public-id");
const record = await stream.getRecord();
await stream.appendMessages([message]);
const tail = await stream.listMessages({ after: record?.currentOffset });
```

Two rules:

1. **`Stream` implements the record and message operations directly.** Do not expose a public `stream.deps.recordStore.getRecord()` dependency bag. Protocol code talks to a stream.
2. **Bound stores have no `streamId` parameter.** Multi-stream tables or maps are fine as an internal implementation detail; bind them to one stream id once, inside the factory.

Optional behaviour is surfaced as additional members on `Stream`:

| Member       | Type                        | Used for                              |
| ------------ | --------------------------- | ------------------------------------- |
| `producers`  | `StreamProducerStore`       | producer-idempotent appends           |
| `references` | `StreamReferenceTracker`    | fork parent/child reference counting  |
| `mutations`  | `StreamMutationCoordinator` | per-stream serialization of mutations |
| `events`     | `StreamEventHub`            | live-read notification                |
| `expiry`     | `StreamExpiryScheduler`     | active expiry scheduling              |

Adapters omit these members when the backend does not support the corresponding behaviour. Protocol methods that cover those behaviours return a structured `not-supported` result (see below) rather than throwing or silently no-opping.

### `composeStream` helper

Factories can assemble a `Stream` from bound dependencies using the `composeStream` helper:

```ts
import { composeStream } from "@streamsy/core";

return composeStream({
  id: streamId,
  recordStore,
  messageStore,
  producerStore,
  referenceTracker,
  mutations,
  events,
  expiry,
});
```

The helper forwards calls to each bound dependency and surfaces only the optional members that were supplied. Adapter authors may also build a `Stream` by hand if that is clearer for the backend.

### Optional behaviour: `NotSupportedResult`

Optional behaviour is represented in protocol results, not as a separate capability negotiation system. When a protocol method covers behaviour the active adapter does not support, the method returns:

```ts
import { notSupported, type NotSupportedResult } from "@streamsy/core";

const result: NotSupportedResult = notSupported("fork", "forks disabled for this stream");
```

The structured result has shape `{ status: "not-supported"; feature: string; message?: string }`. HTTP handlers map it to a 4xx response:

```ts
import { notSupportedResponse, HttpResponseFactory } from "@streamsy/core";

const responses = new HttpResponseFactory();
return notSupportedResponse(result, responses);
```

`notSupportedResponse` returns a 400 Bad Request whose body identifies the unsupported feature and whose `stream-not-supported` header carries the machine-readable feature id.

There is intentionally **no** capability enum or adapter-level capability declaration in the first pass. Capability declarations may be added later if they become useful for configuration, docs generation, or host validation; the source of truth in the meantime is the protocol result.

## Compatibility seam for existing adapters

Existing adapters that implement the multi-stream `StreamStoreAdapter` interface can be exposed as a `StreamFactory` without a rewrite:

```ts
import { createStreamFactoryFromAdapter } from "@streamsy/core";
import { createMemoryStreamStore } from "@streamsy/storage-memory";

const adapter = createMemoryStreamStore();
const factory = createStreamFactoryFromAdapter(adapter);
const stream = await factory.getStream("demo");
```

The shim:

- binds every adapter call to the requested stream id;
- forwards `withLock` to `withMutationLock` under the key `stream:<streamId>`;
- surfaces `events` and `expiry` only when both halves of the pair (`waitForEvent`/`notify`, `scheduleExpiry`/`cancelExpiry`) are implemented;
- never invents capability where the adapter has none.

This shim is the migration path for adapter packages: keep implementing `StreamStoreAdapter`, expose a factory using the shim, and migrate to a native factory at your own pace.

## Adapter author checklist

When designing a new adapter, walk through these questions and decide where each answer lives:

1. **How do I get a stream for a public stream id?** Factory-owned.
2. **Where do this stream's record and messages live?** Factory-owned routing; bound stores inside the stream.
3. **Do I support producer or producer idempotency?** Provide `StreamProducerStore`; otherwise omit it and return `not-supported` from producer-idempotent protocol methods.
4. **How are mutations serialized?** Provide `StreamMutationCoordinator` when the backend has a natural per-stream serialization point (a Durable Object, a row lock, a file lock); otherwise rely on the core in-process fallback.
5. **Do I support live reads / notification?** Provide `StreamEventHub`. Without it, live-read paths that require notification should return `not-supported`; catch-up reads can still work.
6. **Do I support active expiry, or only lazy expiry?** Provide `StreamExpiryScheduler` for active expiry. Without it, the protocol relies on lazy expiry.
7. **Do I support forks, and in what scope?** Forks need `StreamReferenceTracker`. Cross-stream and cross-object fork policy is a factory or engine concern; per-adapter fork policy may also return `not-supported`.
8. **What is cross-stream or factory-scoped rather than single-stream-scoped?** Anything cross-stream (indexes, scans, locks spanning multiple streams) belongs on the factory or an adjacent engine helper, not on the returned `Stream`.

## Worked examples

The architecture is designed to scale across a spectrum of backends:

| Adapter                   | Factory returns                                             | Canonical write path                              | Optional behaviour                                           |
| ------------------------- | ----------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------ |
| Memory (reference)        | `Stream` bound to an entry in a process-local table         | Direct table writes plus an in-process lock chain | Producer store, references, events, expiry — all supported   |
| SQLite                    | `Stream` bound to rows keyed by stream id                   | DB transaction or write lock                      | Producer table, local forks, expiry index, polling or notify |
| Filesystem                | `Stream` bound to an escaped or hashed directory            | File lock plus atomic writes                      | JSON producer map, lazy expiry, local forks                  |
| R2 / KV with co-ord       | `Stream` bound to a key prefix plus an external coordinator | Separate coordinator, DO, or distributed lock     | Read-model or cache modes; forks may be `not-supported`      |
| Embedded domain DO        | `Stream` implemented by host object methods                 | Host object methods plus a stream engine helper   | Host policy for forks, expiry, events                        |
| Cloudflare Durable Object | One-stream-per-DO factory today; many-streams-per-DO later  | DO storage, KV, or SQLite via `ctx.storage`       | DO alarm-backed expiry, in-DO event hub, per-key lock chain  |

The architecture should make new adapters straightforward rather than requiring them to imitate Cloudflare DO internals.

## What stays internal

The factory seam exists alongside the existing public API surface documented in `docs/api.md`. Protocol services, HTTP method services, record factories, lock providers, and conformance internals remain internal. New adapters compose through `composeStream` (or hand-rolled equivalents) and the public types exported from `@streamsy/core`.
