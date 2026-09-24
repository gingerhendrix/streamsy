import { StreamFamily } from "@streamsy/core";
import { sourceStreamId, targetStreamId } from "./config.ts";
import { HackerNewsSourceChange } from "./source-change.ts";

export class HackerNewsSource extends StreamFamily.json<HackerNewsSource>()(
  "HackerNews/Source",
  sourceStreamId,
  { params: {}, schema: HackerNewsSourceChange },
) {}
export class HackerNewsTarget extends StreamFamily.bytes<HackerNewsTarget>()(
  "HackerNews/Target",
  targetStreamId,
  { params: {} },
) {}
/** The application creates the input; the projection owns output creation. */
export const hackerNewsSource = HackerNewsSource.at({});
