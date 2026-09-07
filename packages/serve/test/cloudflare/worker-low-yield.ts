import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Layer } from "effect";
import { MaxOpsBeforeYield } from "effect/Scheduler";
import { router } from "@streamsy/serve/cloudflare";
import { ProbeObject } from "./worker.ts";
import { byStreamOptions } from "./fixture-options.ts";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

class LowYieldObject extends ProbeObject {
  override layer() {
    return super.layer().pipe(Layer.provideMerge(Layer.succeed(MaxOpsBeforeYield, 128)));
  }
}

const app = router<Env>({
  namespace: (env) => env.STREAMS,
  ...byStreamOptions,
});

export default { fetch: app.fetch };
export { LowYieldObject as ProbeObject };
