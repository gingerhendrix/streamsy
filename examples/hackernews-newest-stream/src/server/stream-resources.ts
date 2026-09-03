import { streamIdentity } from "@streamsy/streams/identity";
import * as StateProjection from "@streamsy/projection";
import { sourceStreamId, targetStreamId } from "./config.ts";

export const hackerNewsSource = StateProjection.resource({
  identity: streamIdentity("hacker-news-newest-source"),
  streamId: sourceStreamId,
});

export const hackerNewsTarget = StateProjection.resource({
  identity: streamIdentity("hacker-news-newest-state"),
  streamId: targetStreamId,
});

export const hackerNewsResources = [hackerNewsSource, hackerNewsTarget] as const;
