/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- Workerd binding fixture uses native Worker fetch edges. */
import worker, { type CloudflareEnv } from "../server/host/cloudflare/cloudflare.ts";
import type { ExecutionContext } from "@cloudflare/workers-types";

const immutableAssetBinding = {
  fetch: async (_request: Request): Promise<Response> => {
    const response = await fetch("data:text/plain,immutable-asset");
    if (response.headers.get("content-type") === null) throw new Error("missing fixture header");
    return response;
  },
};

export default {
  fetch(request: Request, env: CloudflareEnv, ctx: ExecutionContext): Promise<Response> {
    return worker.fetch(request, { ...env, ASSETS: immutableAssetBinding }, ctx);
  },
};

export { WorkspacePartitionObject } from "../server/host/cloudflare/cloudflare.ts";
