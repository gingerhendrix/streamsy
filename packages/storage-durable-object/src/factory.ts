/**
 * Native Durable Object `StreamFactory`.
 *
 * Implements the one-stream-per-Durable-Object model: each public stream id is
 * routed to a per-stream `DurableObjectStreamStorage` instance via
 * `namespace.idFromName(streamId)`. The Durable Object owns persistent stream
 * state and exposes the storage RPC methods; this module returns the small
 * protocol-facing proxy needed for local-only `Stream` concerns (`id` and
 * callback-based mutation locking) that do not map cleanly to Cloudflare RPC.
 */
import type { Stream, StreamFactory, StreamId } from "@streamsy/core";
import { DurableObjectStreamProxy } from "./proxy.ts";
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
      return new DurableObjectStreamProxy(streamId, stub);
    },
  };
}
