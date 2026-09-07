import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Placement, router } from "@streamsy/serve/cloudflare";
import { ProbeObject } from "./worker.ts";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const app = router<Env>({
  namespace: (env) => env.STREAMS,
  pathPrefix: "/streams",
  placement: Placement.byKey((streamPath) => streamPath.split("/", 1)[0] ?? ""),
});

export default { fetch: app.fetch };
export { ProbeObject };
