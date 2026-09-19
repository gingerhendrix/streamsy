import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { router } from "@streamsy/serve/cloudflare";
import { ProbeByKeyObject } from "./worker.ts";
import { byKeyOptions } from "./fixture-options.ts";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

const app = router<Env>({
  namespace: (env) => env.STREAMS,
  ...byKeyOptions,
});

export default { fetch: app.fetch };
export { ProbeByKeyObject as ProbeObject };
