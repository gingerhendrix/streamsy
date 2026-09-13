import { Context } from "effect";
import type { HttpOptions } from "@streamsy/core/http";

/** HTTP configuration supplied by the object's construction Layer. */
export class ObjectOptions extends Context.Service<ObjectOptions, HttpOptions>()(
  "@streamsy/serve/cloudflare/ObjectOptions",
) {}
