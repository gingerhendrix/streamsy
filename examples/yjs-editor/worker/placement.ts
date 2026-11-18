import { Placement, rule } from "@streamsy/serve/cloudflare";
import { Awareness, AwarenessIndex, Snapshots, Updates } from "./streams.ts";

/** One object owns every stream of a document. */
export const placement = Placement.byRoute([
  rule({ stream: Updates, owner: ({ document }) => document }),
  rule({ stream: Snapshots, owner: ({ document }) => document }),
  rule({ stream: Awareness, owner: ({ document }) => document }),
  rule({ stream: AwarenessIndex, owner: ({ document }) => document }),
]);
