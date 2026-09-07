import { Context } from "effect";

export class HostCommand extends Context.Service<HostCommand, { readonly _tag: "ExpireDue" }>()(
  "@streamsy/serve/cloudflare/HostCommand",
) {}
