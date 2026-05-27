/**
 * Native Durable Object `StreamFactory`.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id
 * is routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`, and `composeStream(...)` binds the
 * record/message/producer/reference/runtime operations of that DO to a
 * protocol-facing `Stream` for that id.
 *
 * The returned `Stream` does not expose a public dependency bag. Namespace
 * routing, stub acquisition, and lock-key construction are factory-owned
 * concerns and stay internal.
 *
 * The existing {@link DurableObjectStreamStoreAdapter} remains the
 * StreamStoreAdapter-shaped entry point used by `StreamProtocol`. The new
 * factory is an additive surface for adapter authors and downstream code
 * targeting the factory/composed-stream seam directly. Both APIs talk to the
 * same `DurableObjectStreamStorage` class so the persisted data model is
 * unchanged.
 */
import {
  composeStream,
  type Stream,
  type StreamEventType,
  type StreamFactory,
  type StreamId,
  type WaitForEventOptions,
} from "@streamsy/core";
import type { DurableObjectStreamStorage } from "./storage.ts";

export interface DurableObjectStreamFactoryOptions {
  namespace: DurableObjectNamespace<DurableObjectStreamStorage>;
}

export function createDurableObjectStreamFactory(
  options: DurableObjectStreamFactoryOptions,
): StreamFactory {
  const { namespace } = options;
  const stubFor = (streamId: StreamId) => namespace.get(namespace.idFromName(streamId));

  return {
    getStream(streamId: StreamId): Stream {
      return composeStream({
        id: streamId,
        recordStore: {
          getRecord: () => stubFor(streamId).get(streamId),
          createRecord: (record) => stubFor(streamId).create(record),
          updateRecord: (patch) => stubFor(streamId).update(streamId, patch),
          deleteRecord: () => stubFor(streamId).delete(streamId),
        },
        messageStore: {
          appendMessages: (messages) => stubFor(streamId).append(streamId, messages),
          listMessages: (listOptions) => stubFor(streamId).list(streamId, listOptions),
          deleteMessages: () => stubFor(streamId).deleteMessages(streamId),
        },
        producerStore: {
          getProducerState: (producerId) =>
            stubFor(streamId).getProducerState(streamId, producerId),
          setProducerState: (producerId, state) =>
            stubFor(streamId).setProducerState(streamId, producerId, state),
          deleteProducerStates: () => stubFor(streamId).deleteProducerStates(streamId),
        },
        referenceTracker: {
          incrementChildRefCount: () => stubFor(streamId).incrementChildRefCount(streamId),
          decrementChildRefCount: () => stubFor(streamId).decrementChildRefCount(streamId),
        },
        mutations: {
          withMutationLock: async <T>(fn: () => Promise<T>): Promise<T> => {
            // The DO is the natural per-stream serialization point. Use the
            // same lock key shape (`stream:<id>`) as
            // {@link DurableObjectStreamStoreAdapter} so the two surfaces
            // share the in-DO lock chain for the same stream.
            const key = `stream:${streamId}`;
            const stub = stubFor(streamId);
            const token = await stub.acquireLock(key);
            try {
              return await fn();
            } finally {
              await stub.releaseLock(key, token);
            }
          },
        },
        events: {
          waitForEvent: (waitOptions: WaitForEventOptions) =>
            stubFor(streamId).waitForEvent(streamId, waitOptions),
          notify: (type: StreamEventType) => stubFor(streamId).notify(streamId, type),
        },
        expiry: {
          scheduleExpiry: (at: number) => stubFor(streamId).scheduleExpiry(streamId, at),
          cancelExpiry: () => stubFor(streamId).cancelExpiry(streamId),
        },
      });
    },
  };
}
