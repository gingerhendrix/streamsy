import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import type { Layer } from "effect";
import type { Storage, StorageFault, StreamsReader, StreamsWriter } from "@streamsy/core";
import { Placement, StreamsyObject, router, type ObjectOptions } from "@streamsy/serve/cloudflare";
import { layerProtocol } from "@streamsy/storage/durable-object";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const host = { pathPrefix: "/streams" } as const;

export class StreamsObject extends StreamsyObject<Env> {
  override layer(): Layer.Layer<StreamsReader | StreamsWriter | Storage, StorageFault> {
    return layerProtocol({ client: { storage: this.ctx.storage } });
  }

  override options(): ObjectOptions {
    return host;
  }
}

export default router<Env>({
  ...host,
  placement: Placement.byStream(),
  namespace: (env) => env.STREAMS,
});
