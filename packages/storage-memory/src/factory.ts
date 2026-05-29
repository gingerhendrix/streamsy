/**
 * Native memory `StreamFactory`.
 *
 * Owns a process-wide multi-stream state table (`MemoryStreamState`). Each
 * lookup asks that table for a stream-oriented handle and composes the
 * protocol-facing `Stream` from simple bound stores: records, messages,
 * producers, references, mutation coordination, events, and expiry.
 *
 * This keeps the reference adapter faithful to the factory seam. The returned
 * stream is bound to one id, and no id-routing or dependency bag leaks through
 * the public adapter API.
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
    async getStream(streamId: StreamId): Promise<Stream> {
      const stores = state.stream(streamId);
      return composeStream({
        id: streamId,
        recordStore: stores.records,
        messageStore: stores.messages,
        producerStore: stores.producers,
        referenceTracker: stores.references,
        mutations: stores.mutations,
        events: stores.events,
        expiry: stores.expiry,
      });
    },
  };
}
