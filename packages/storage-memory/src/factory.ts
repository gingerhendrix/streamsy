/**
 * Native memory `StreamFactory`.
 *
 * Owns a process-wide registry of `MemoryStream` instances. Each lookup returns
 * the protocol-facing stream object for one id; `MemoryStream` implements the
 * core `Stream` interface directly and delegates storage/runtime operations to
 * simple bound stores.
 */
import type { Stream, StreamFactory, StreamId } from "@streamsy/core";
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
      return state.getStream(streamId);
    },
  };
}
