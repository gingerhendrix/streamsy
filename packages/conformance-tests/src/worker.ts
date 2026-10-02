import { Streamsy, Http } from "@streamsy/core";
import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Placement, StreamsyObject, router } from "@streamsy/serve/cloudflare";
import { layer } from "@streamsy/storage/durable-object";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const host = {
  prefix: "/",
} as const;

const streams = Streamsy.group(Streamsy.Wildcard);

export class StreamsObject extends StreamsyObject.make<Env, Streamsy.Services<typeof streams>>({
  app: Http.routes(streams, { prefix: host.prefix, access: "full" }),
  layer: (state) =>
    layer(streams, { client: { storage: state.storage }, longPollTimeoutMs: 1_500 }),
}) {}

export default router<Env>({
  ...host,
  placement: Placement.byKey(() => "conformance"),
  namespace: (env) => env.STREAMS,
});
