/**
 * Native memory `StreamFactory`.
 *
 * Owns a process-wide multi-stream table (`MemoryStreamStore`) and composes
 * a protocol-facing `Stream` per id from that table using `composeStream`.
 * Bound record/message/producer/reference/runtime operations target one
 * stream id, fixed at composition time, so the returned `Stream` carries no
 * public dependency bag.
 *
 * This is the reference implementation of the factory seam for new adapter
 * authors: small, in-memory, and faithful to the architecture.
 */
import { composeStream, type Stream, type StreamFactory, type StreamId } from "@streamsy/core";
import { MemoryStreamStore } from "./storage.ts";

export interface MemoryStreamFactoryOptions {
  /**
   * Share an existing memory state. Useful in tests and examples that need
   * a `StreamStoreAdapter` and a `StreamFactory` over the same in-process
   * data. Omit to get a fresh, isolated state.
   */
  state?: MemoryStreamStore;
}

export function createMemoryStreamFactory(options: MemoryStreamFactoryOptions = {}): StreamFactory {
  const state = options.state ?? new MemoryStreamStore();
  return {
    getStream(streamId: StreamId): Stream {
      return composeStream({
        id: streamId,
        recordStore: {
          getRecord: () => state.get(streamId),
          createRecord: (record) => state.create(record),
          updateRecord: (patch) => state.update(streamId, patch),
          deleteRecord: () => state.delete(streamId),
        },
        messageStore: {
          appendMessages: (messages) => state.append(streamId, messages),
          listMessages: (listOptions) => state.list(streamId, listOptions),
          deleteMessages: () => state.deleteMessages(streamId),
        },
        producerStore: {
          getProducerState: (producerId) => state.getProducerState(streamId, producerId),
          setProducerState: (producerId, producerState) =>
            state.setProducerState(streamId, producerId, producerState),
          deleteProducerStates: () => state.deleteProducerStates(streamId),
        },
        referenceTracker: {
          incrementChildRefCount: () => state.incrementChildRefCount(streamId),
          decrementChildRefCount: () => state.decrementChildRefCount(streamId),
        },
        mutations: {
          withMutationLock: (fn) => state.withLock(`stream:${streamId}`, fn),
        },
        events: {
          waitForEvent: (waitOptions) => state.waitForEvent(streamId, waitOptions),
          notify: (type) => state.notify(streamId, type),
        },
        expiry: {
          scheduleExpiry: (at, callback) => state.scheduleExpiry(streamId, at, callback),
          cancelExpiry: () => state.cancelExpiry(streamId),
        },
      });
    },
  };
}
