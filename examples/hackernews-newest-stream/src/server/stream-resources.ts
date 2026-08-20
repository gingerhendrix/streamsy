import { streamIdentity } from "@streamsy/experimental/causal";
import { StateProjection } from "@streamsy/experimental/effect/state-projection";
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
