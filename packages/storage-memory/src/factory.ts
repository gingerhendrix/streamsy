/**
 * Native memory `StreamFactory`.
 *
 * Owns a process-wide multi-stream table (`MemoryStreamState`) and composes
 * a protocol-facing `Stream` per id from that table using `composeStream`.
 * Bound record/message/producer/reference/runtime operations target one
 * stream id, fixed at composition time, so the returned `Stream` carries no
 * public dependency bag.
 *
 * This is the reference implementation of the factory seam: small, in-memory,
 * and faithful to the architecture.
 */
import { composeStream, type Stream, type StreamFactory, type StreamId } from "@streamsy/core";
import { MemoryStreamState } from "./storage.ts";

export interface MemoryStreamFactoryOptions {
  /**
   * Share an existing memory state. Omit to get a fresh, isolated state.
   */
  state?: MemoryStreamState;
}

export function createMemoryStreamFactory(options: MemoryStreamFactoryOptions = {}): StreamFactory {
  const state = options.state ?? new MemoryStreamState();
  return {
    getStream(streamId: StreamId): Stream {
      return composeStream({
        id: streamId,
        recordStore: {
          getRecord: () => state.get(streamId),
          createRecord: (record) => state.create(record),
          updateRecord: (patch) => state.update(streamId, patch),
          deleteRecord: () => state.deleteStream(streamId),
        },
        messageStore: {
          appendMessages: (messages) => state.appendToStream(streamId, messages),
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
