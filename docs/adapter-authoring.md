# Storage factory authoring

Streamsy storage packages implement `StreamFactory`.

A `StreamFactory` maps a public stream id to a storage-bound `Stream`. The returned storage stream is bound to one id and exposes the protocol storage/runtime operations for that id as direct methods:

- record operations: `getRecord`, `createRecord`, `updateRecord`, `deleteRecord`
- message operations: `appendMessages`, `listMessages`, `deleteMessages`
- producer state: `getProducerState`, `setProducerState`, `deleteProducerStates`
- fork references: `incrementChildRefCount`, `decrementChildRefCount`
- mutation coordination: `withMutationLock`
- live events: `waitForEvent`, `notify`
- expiry scheduling: `scheduleExpiry`, `cancelExpiry`

Facet interfaces such as `StreamRecordStore`, `StreamMessageStore`, and `StreamProducerStore` are available for internal implementation structure, tests, and documentation. They are not exposed as nested properties on `Stream`: adapters should return one object implementing `Stream` and may delegate internally to private stores.

```ts
import type { Stream, StreamFactory } from "@streamsy/core";

class ExampleStream implements Stream {
  constructor(
    readonly id: string,
    private readonly records: ExampleRecordStore,
    private readonly messages: ExampleMessageStore,
    private readonly producers: ExampleProducerStore,
  ) {}

  getRecord() {
    return this.records.getRecord();
  }

  appendMessages(messages) {
    return this.messages.appendMessages(messages);
  }

  getProducerState(producerId) {
    return this.producers.getProducerState(producerId);
  }

  // Implement the remaining Stream methods directly, delegating to private
  // stores or runtime helpers as appropriate.
}

export function createExampleStreamFactory(): StreamFactory {
  return {
    async getStream(id) {
      return new ExampleStream(
        id,
        new ExampleRecordStore(id),
        new ExampleMessageStore(id),
        new ExampleProducerStore(id),
      );
    },
  };
}
```

Unsupported protocol features should be explicit method-level behavior. A minimal adapter must still provide every `Stream` method; unsupported methods should throw the typed storage-level unsupported error (`unsupported(...)` / `NotSupportedError`) or use an equivalent adapter-local helper that the protocol layer can map to the public structured `not-supported` result.

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

The Durable Object package uses one storage object per public stream id. The host-facing entrypoint is `createDurableObjectStreamFactory({ namespace })`; the factory returns a `Stream` backed by a stream-bound Durable Object stub. Durable Object instance methods do not need a redundant `streamId` argument because the namespace binding already selected the stream.
