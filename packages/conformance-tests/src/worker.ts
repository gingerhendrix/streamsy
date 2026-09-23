import { Http } from "@streamsy/core";
import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Placement, StreamsyObject, router } from "@streamsy/serve/cloudflare";
import { layerProtocol } from "@streamsy/storage/durable-object";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const host = {
  prefix: "/",
} as const;

export class StreamsObject extends StreamsyObject.make<Env>({
  app: Http.routes({ prefix: host.prefix }),
  layer: (state) => layerProtocol({ client: { storage: state.storage }, longPollTimeoutMs: 1_500 }),
}) {}

export default router<Env>({
  ...host,
  placement: Placement.byKey(() => "conformance"),
  namespace: (env) => env.STREAMS,
});
