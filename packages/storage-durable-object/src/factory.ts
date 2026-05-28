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
 * The factory is the host-facing entry point used by the protocol factory.
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
    async getStream(streamId: StreamId): Promise<Stream> {
      return composeStream({
        id: streamId,
        recordStore: {
          getRecord: () => stubFor(streamId).get(streamId),
          createRecord: (record) => stubFor(streamId).create(record),
          updateRecord: (patch) => stubFor(streamId).update(streamId, patch),
          deleteRecord: () => stubFor(streamId).deleteStream(streamId),
        },
        messageStore: {
          appendMessages: (messages) => stubFor(streamId).appendToStream(streamId, messages),
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
            // The DO is the natural per-stream serialization point.
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
