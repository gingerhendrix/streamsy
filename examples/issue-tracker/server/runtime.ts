/**
 * The application layer, assembled once.
 *
 * This is the only place the dependency graph is written down. A host chooses
 * storage for the log, a store for the maintained state, and a configuration
 * source; everything else is fixed by the declaration.
 *
 * Swapping `store` between `memoryLayer()` and `sqliteLayer(...)` is the whole
 * difference between the in-memory host and the durable one — the declaration,
 * the plan, the engine, the sink and the router are identical.
 */
import type { StreamProtocolClient, StreamProtocolFactory } from "@streamsy/core";
import { Layer } from "effect";
import { MeshLayer, type ApplicationServices } from "./application.ts";
import * as Commands from "./commands.ts";
import type { AppConfig } from "./config.ts";
import * as GatewayModule from "./gateway.ts";
import type { StreamGateway } from "./gateway.ts";
import { sinkLayer } from "./sink.ts";
import type { IssueStore } from "./store.ts";
import * as StreamsModule from "./streams.ts";

export interface ApplicationLayerOptions {
  /** Storage resolved into a protocol client by the host. */
  readonly client: StreamProtocolClient;
  /** The same storage as a protocol factory, for the Durable State sink writer. */
  readonly protocol: StreamProtocolFactory;
  /** The Durable Streams HTTP handler the sink route borrows. */
  readonly gateway: { readonly fetch: (request: Request) => Promise<Response> };
  readonly config: Layer.Layer<AppConfig>;
  /** Where maintained rows, reducer state, receipts and progress live. */
  readonly store: Layer.Layer<IssueStore>;
}

export const applicationLayer = (
  options: ApplicationLayerOptions,
): Layer.Layer<ApplicationServices | StreamGateway> =>
  Layer.mergeAll(
    MeshLayer,
    StreamsModule.layer(options.client),
    Commands.layer,
    options.store,
    GatewayModule.layer(options.gateway),
    sinkLayer(options.protocol),
    options.config,
  );
