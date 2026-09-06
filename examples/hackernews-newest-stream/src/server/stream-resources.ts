import * as StateProjection from "./bridge/state-projection.ts";
import { sourceStreamId, targetStreamId } from "./config.ts";

export const hackerNewsSource = StateProjection.resource({
  streamId: sourceStreamId,
});

export const hackerNewsTarget = StateProjection.resource({
  streamId: targetStreamId,
});

export const hackerNewsResources = [hackerNewsSource, hackerNewsTarget] as const;
