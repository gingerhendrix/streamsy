/**
 * The application layer, assembled once.
 *
 * This is the only place the application's dependency graph is written down.
 * A host chooses storage, configuration, and a wake lane; everything else —
 * mesh capabilities, producer lanes, command producers — is fixed.
 *
 * The layer is flat and topologically ordered on purpose: `MeshLayer` and
 * `Streams` are independent, `ProjectionLanes` and `CommandProducers` depend on
 * nothing, and the host-chosen pieces are the only parameters.
 */
import type { StreamProtocolClient } from "@streamsy/core";
import { Layer } from "effect";
import { MeshLayer, type ApplicationServices } from "./application.ts";
import * as Commands from "./commands.ts";
import { AppConfig } from "./config.ts";
import * as Lanes from "./lanes.ts";
import * as StreamsModule from "./streams.ts";
import { Wake } from "./wake.ts";

export interface ApplicationLayerOptions {
  /** Storage resolved into a protocol client by the host. */
  readonly client: StreamProtocolClient;
  /** Where configuration comes from: `Config`, or a concrete value. */
  readonly config: Layer.Layer<AppConfig>;
  /** The host's background convergence lane, if it has one. */
  readonly wake: Layer.Layer<Wake>;
}

/** Build the complete application layer for one host. */
export const applicationLayer = (
  options: ApplicationLayerOptions,
): Layer.Layer<ApplicationServices> =>
  Layer.mergeAll(
    MeshLayer,
    StreamsModule.layer(options.client),
    Lanes.layer,
    Commands.layer,
    options.config,
    options.wake,
  );
