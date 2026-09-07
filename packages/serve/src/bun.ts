// oxlint-disable effecttsgo/async-function -- Named Web/Bun boundary owns native request, response and server disposal operations.
import { Effect, type Layer } from "effect";
import type { StreamsReader, StreamsWriter } from "@streamsy/core";
import { makeEdge, type HttpOptions } from "@streamsy/core/http";

export interface ServeOptions<E = never> extends HttpOptions {
  readonly layer: Layer.Layer<StreamsReader | StreamsWriter, E>;
  readonly port?: number;
  readonly hostname?: string;
}

/** Named Bun executable edge. It owns the listener and the Effect layer lifetime. */
export async function serve<E>(options: ServeOptions<E>) {
  const edge = makeEdge(options, options.layer);
  const activeRequests = new Set<Promise<Response>>();
  const handle = (request: Request): Promise<Response> => {
    let tracked: Promise<Response>;
    tracked = edge.handler(request).finally(() => {
      activeRequests.delete(tracked);
    });
    activeRequests.add(tracked);
    return tracked;
  };
  let server: ReturnType<typeof Bun.serve>;
  try {
    server = Bun.serve({
      port: options.port ?? 3000,
      hostname: options.hostname ?? "127.0.0.1",
      idleTimeout: 0,
      fetch: handle,
    });
  } catch (error) {
    await edge.dispose();
    throw error;
  }
  let stopping: Promise<void> | undefined;
  return {
    port: server.port,
    url: server.url,
    stop: () =>
      (stopping ??= (async () => {
        try {
          await server.stop(true);
          await Promise.allSettled(activeRequests);
          await Effect.runPromise(edge.awaitIdle);
        } finally {
          await edge.dispose();
        }
      })()),
  };
}
