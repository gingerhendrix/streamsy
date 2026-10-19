import { Placement } from "@streamsy/serve/cloudflare";

export const byStreamOptions = {
  prefix: "/streams",
  placement: Placement.byStream(),
} as const;

export const byKeyOptions = {
  prefix: "/streams",
  placement: Placement.byKey((streamPath: string) => streamPath.split("/", 1)[0] ?? ""),
} as const;
