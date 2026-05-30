/**
 * Native Durable Object `StreamFactory`.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id is
 * routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`. The Durable Object itself implements the
 * protocol-facing storage methods; the factory initializes the routed stub and
 * annotates that same stub with local identity/lock helpers required by the
 * direct `Stream` interface.
 */
import type { Stream, StreamFactory, StreamId } from "@streamsy/core";
import type { DurableObjectStreamStorage } from "./storage.ts";

export interface DurableObjectStreamFactoryOptions {
  namespace: DurableObjectNamespace<DurableObjectStreamStorage>;
}

export function createDurableObjectStreamFactory(
  options: DurableObjectStreamFactoryOptions,
): StreamFactory {
  const { namespace } = options;

  return {
    async getStream(streamId: StreamId): Promise<Stream> {
      const stub = namespace.get(namespace.idFromName(streamId));
      await stub.init(streamId);
      return bindStreamSurface(streamId, stub);
    },
  };
}

function bindStreamSurface(
  streamId: StreamId,
  stub: DurableObjectStub<DurableObjectStreamStorage>,
): Stream {
  Object.defineProperties(stub, {
    id: { value: streamId, configurable: true },
    withMutationLock: {
      value: async <T>(fn: () => Promise<T>): Promise<T> => {
        const key = `stream:${streamId}`;
        const token = await stub.acquireLock(key);
        try {
          return await fn();
        } finally {
          await stub.releaseLock(key, token);
        }
      },
      configurable: true,
    },
  });
  return stub as unknown as Stream;
}
