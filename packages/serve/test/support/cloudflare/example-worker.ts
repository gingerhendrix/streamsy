import { AnyStream, Http, StreamGroup } from "@streamsy/core";
import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Placement, StreamsyObject, router } from "@streamsy/serve/cloudflare";
import { layer } from "@streamsy/storage/durable-object";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const host = { prefix: "/streams" } as const;

const streams = StreamGroup.make(AnyStream);

export class StreamsObject extends StreamsyObject.make<Env, StreamGroup.Services<typeof streams>>({
  app: Http.routes(streams, { prefix: host.prefix, access: "full" }),
  layer: (state) => layer({ client: { storage: state.storage } }, streams),
}) {}

export default router<Env>({
  ...host,
  placement: Placement.byStream(),
  namespace: (env) => env.STREAMS,
});
