import { router, type ObjectOptions } from "@streamsy/serve/cloudflare";
import { ProbeObject } from "./worker.ts";
import { byStreamOptions } from "./fixture-options.ts";

interface Env {
  readonly STREAMS: import("@cloudflare/workers-types").DurableObjectNamespace;
}

class NoNamespaceObject extends ProbeObject {
  override options(): ObjectOptions<Env> {
    return { ...byStreamOptions };
  }
}

const app = router<Env>({ namespace: (env) => env.STREAMS, ...byStreamOptions });

export default { fetch: app.fetch };
export { NoNamespaceObject as ProbeObject };
