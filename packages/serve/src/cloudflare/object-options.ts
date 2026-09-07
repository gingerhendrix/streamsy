import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import type { HttpOptions } from "@streamsy/core/http";
import type { Placement } from "./placement.ts";

export const DEFAULT_COPY_ON_FORK_MAX_BYTES = 8 * 1024 * 1024;

export const validateCopyOnForkMaxBytes = (value: number | undefined): number => {
  const resolved = value ?? DEFAULT_COPY_ON_FORK_MAX_BYTES;
  if (!Number.isSafeInteger(resolved) || resolved <= 0)
    throw new RangeError("copyOnForkMaxBytes must be a positive safe integer");
  return resolved;
};

export interface ObjectOptions<Env = unknown> extends HttpOptions {
  readonly placement?: Placement;
  readonly namespace?: (env: Env) => DurableObjectNamespace;
  readonly copyOnForkMaxBytes?: number;
}
