declare module "cloudflare:workers" {
  import type { DurableObjectState } from "@cloudflare/workers-types";

  export class DurableObject<Env = unknown> {
    protected readonly ctx: DurableObjectState;
    protected readonly env: Env;
    constructor(ctx: DurableObjectState, env: Env);
  }
}
