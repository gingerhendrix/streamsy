/**
 * Durable Streams Server
 *
 * Worker entry point and composition root.
 * Wires together the HTTP handler, protocol, and storage layers.
 */

import type { StreamStorage as StreamStorageInterface } from "@streamsy/core";
import { HttpHandler, StreamProtocol } from "@streamsy/core";
import { DurableObjectStreamStorage as StreamStorage } from "@streamsy/storage-durable-object";

// Re-export the Durable Object class for the runtime.
// The exported name must match `className` in alchemy.run.ts.
export { StreamStorage };

/**
 * Environment type for the worker.
 * Uses the StreamStorage class for proper DO typing.
 */
interface Env {
  STREAM_DO: DurableObjectNamespace<StreamStorage>;
}

/**
 * Worker entry point
 *
 * Creates the storage factory, protocol, and HTTP handler.
 * This is the composition root where all DI happens.
 */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Create storage factory from env binding
    const storageFactory = (streamId: string): StreamStorageInterface => {
      const id = env.STREAM_DO.idFromName(streamId);
      // The Durable Object stub has all the methods of StreamStorage
      return env.STREAM_DO.get(id) as unknown as StreamStorageInterface;
    };

    // Create protocol with factory
    const protocol = new StreamProtocol(storageFactory);

    // Create handler with protocol
    const handler = new HttpHandler({ protocol });

    // Execute request
    return handler.fetch(request);
  },
} satisfies ExportedHandler<Env>;
