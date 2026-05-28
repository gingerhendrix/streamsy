# Storage factory authoring

Streamsy storage packages implement `StreamFactory`.

A `StreamFactory` maps a public stream id to a storage-bound `Stream`. The returned storage stream is bound to one id and exposes only primitive persistence/runtime operations for that id:

- record operations: `getRecord`, `createRecord`, `updateRecord`, `deleteRecord`
- message operations: `appendMessages`, `listMessages`, `deleteMessages`
- optional producer state, fork references, mutation coordination, live events, and expiry scheduling

Use `composeStream` when a package has separate record/message stores and wants to assemble the standard storage-bound stream shape.

```ts
import { composeStream, type StreamFactory } from "@streamsy/core";

export function createExampleStreamFactory(): StreamFactory {
  return {
    getStream(id) {
      return composeStream({
        id,
        recordStore,
        messageStore,
        producerStore,
        referenceTracker,
        mutations,
        events,
        expiry,
      });
    },
  };
}
```

Optional capabilities should be omitted when unsupported. Protocol code maps missing optional capabilities to structured `not-supported` results through the shared `require*` helpers.

## Protocol wiring

Applications pass storage through the protocol factory dependency object:

```ts
const protocol = createStreamProtocol({
  storage: { factory: createExampleStreamFactory() },
  longPollTimeoutMs: 1500,
});
```

The protocol layer creates protocol-bound streams with `protocol.create(...)` and `protocol.get(...)`. Storage-bound streams remain an internal persistence boundary and should not be exposed as the HTTP/application protocol object.

## Durable Object guidance

The Durable Object package uses one storage object per public stream id. The host-facing entrypoint is `createDurableObjectStreamFactory({ namespace })`; the Durable Object class supplies the RPC methods used by that factory and the native alarm path.
