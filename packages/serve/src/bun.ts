// oxlint-disable effecttsgo/async-function -- Named Web/Bun boundary owns native request, response and server disposal operations.
import type { Layer } from "effect";
import type { StreamsReader, StreamsWriter } from "@streamsy/core-next";
import { makeEdge, type HttpOptions } from "@streamsy/core-next/http";

export interface ServeOptions<E = never> extends HttpOptions {
  readonly layer: Layer.Layer<StreamsReader | StreamsWriter, E>;
  readonly port?: number;
  readonly hostname?: string;
}

/** Named Bun executable edge. It owns the listener and the Effect layer lifetime. */
export async function serve<E>(options: ServeOptions<E>) {
  const edge = makeEdge(options, options.layer);
  let server: ReturnType<typeof Bun.serve>;
  try {
    server = Bun.serve({
      port: options.port ?? 3000,
      hostname: options.hostname ?? "127.0.0.1",
      idleTimeout: 0,
      fetch: (request) => edge.handler(request),
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
        } finally {
          await edge.dispose();
        }
      })()),
  };
}
