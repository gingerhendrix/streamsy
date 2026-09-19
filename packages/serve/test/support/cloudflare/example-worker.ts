import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Placement, StreamsyObject, router } from "@streamsy/serve/cloudflare";
import { layerProtocol } from "@streamsy/storage/durable-object";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const host = { pathPrefix: "/streams" } as const;

export class StreamsObject extends StreamsyObject.make<Env>({
  options: host,
  layer: (state) => layerProtocol({ client: { storage: state.storage } }),
}) {}

export default router<Env>({
  ...host,
  placement: Placement.byStream(),
  namespace: (env) => env.STREAMS,
});
