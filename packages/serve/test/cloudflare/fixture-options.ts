import { Placement } from "@streamsy/serve/cloudflare";

export const byStreamOptions = {
  pathPrefix: "/streams",
  placement: Placement.byStream(),
} as const;

export const byKeyOptions = {
  pathPrefix: "/streams",
  placement: Placement.byKey((streamPath: string) => streamPath.split("/", 1)[0] ?? ""),
} as const;
