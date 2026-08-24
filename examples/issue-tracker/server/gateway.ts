/**
 * The raw Durable Streams HTTP surface, as a service.
 *
 * The sink route does not re-implement the streams protocol: it authorizes the
 * request, translates its own resume token into a protocol offset, and hands
 * the rewritten request to the same handler the host serves under `/streams`.
 * Owning the route while borrowing the transport is what makes the sink a
 * public contract rather than a second protocol.
 */
import { Context, Effect, Layer } from "effect";

export interface StreamGatewayService {
  /** Serve one Durable Streams HTTP request. Paths are `/streams/...`. */
  readonly fetch: (request: Request) => Effect.Effect<Response>;
  /** Prefix the gateway serves, so callers can build a path without guessing. */
  readonly prefix: string;
}

export class StreamGateway extends Context.Service<StreamGateway, StreamGatewayService>()(
  "issue-tracker/StreamGateway",
) {}

export const layer = (
  handler: { readonly fetch: (request: Request) => Promise<Response> },
  prefix = "/streams",
): Layer.Layer<StreamGateway> =>
  Layer.succeed(
    StreamGateway,
    StreamGateway.of({
      prefix,
      fetch: Effect.fn("StreamGateway.fetch")((request: Request) =>
        Effect.promise(() => handler.fetch(request)),
      ),
    }),
  );
