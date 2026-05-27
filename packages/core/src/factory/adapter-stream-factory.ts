/**
 * Compatibility shim that exposes an existing `StreamStoreAdapter` as a
 * `StreamFactory`. The returned factory binds every operation to a single
 * stream id so protocol-facing code can use the factory/composed-stream
 * seam without rewriting `StreamStoreAdapter` implementations in the same
 * pass.
 *
 * Optional behaviours (mutation lock, live-read events, active expiry) are
 * surfaced on the composed `Stream` only when the underlying adapter
 * implements both halves of the pair. They remain `undefined` otherwise so
 * future protocol code can detect unsupported features without inspecting
 * adapter internals.
 */
import type { StreamFactory } from "../types/factory.ts";
import type { StreamId, StreamStoreAdapter } from "../types/storage.ts";
import { composeStream } from "./compose-stream.ts";

export function createStreamFactoryFromAdapter(adapter: StreamStoreAdapter): StreamFactory {
  return {
    getStream(streamId: StreamId) {
      const mutations = adapter.withLock
        ? {
            withMutationLock: <T>(fn: () => Promise<T>): Promise<T> =>
              adapter.withLock!(`stream:${streamId}`, fn),
          }
        : undefined;
      const events =
        adapter.waitForEvent && adapter.notify
          ? {
              waitForEvent: (
                options: Parameters<NonNullable<StreamStoreAdapter["waitForEvent"]>>[1],
              ) => adapter.waitForEvent!(streamId, options),
              notify: (type: Parameters<NonNullable<StreamStoreAdapter["notify"]>>[1]) =>
                adapter.notify!(streamId, type),
            }
          : undefined;
      const expiry =
        adapter.scheduleExpiry && adapter.cancelExpiry
          ? {
              scheduleExpiry: (at: number, callback?: () => Promise<void>) =>
                adapter.scheduleExpiry!(streamId, at, callback),
              cancelExpiry: () => adapter.cancelExpiry!(streamId),
            }
          : undefined;
      return composeStream({
        id: streamId,
        recordStore: {
          getRecord: () => adapter.get(streamId),
          createRecord: (record) => adapter.create(record),
          updateRecord: (patch) => adapter.update(streamId, patch),
          deleteRecord: () => adapter.delete(streamId),
        },
        messageStore: {
          appendMessages: (messages) => adapter.append(streamId, messages),
          listMessages: (options) => adapter.list(streamId, options),
          deleteMessages: () => adapter.deleteMessages(streamId),
        },
        producerStore: {
          getProducerState: (producerId) => adapter.getProducerState(streamId, producerId),
          setProducerState: (producerId, state) =>
            adapter.setProducerState(streamId, producerId, state),
          deleteProducerStates: () => adapter.deleteProducerStates(streamId),
        },
        referenceTracker: {
          incrementChildRefCount: () => adapter.incrementChildRefCount(streamId),
          decrementChildRefCount: () => adapter.decrementChildRefCount(streamId),
        },
        mutations,
        events,
        expiry,
      });
    },
  };
}
