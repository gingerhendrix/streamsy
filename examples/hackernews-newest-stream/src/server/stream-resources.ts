import { StreamRef } from "@streamsy/core";
import { sourceStreamId, targetStreamId } from "./config.ts";
import { HackerNewsSourceChange } from "./source-change.ts";
import { HackerNewsStory } from "../state-schema.ts";

/** The projection input: reconciliation commands from the poller. */
export const hackerNewsSource = StreamRef.json(sourceStreamId, { schema: HackerNewsSourceChange });

/** The public Durable State target consumed by createStreamDB in the browser. */
export const hackerNewsTarget = StreamRef.state(targetStreamId, {
  schema: HackerNewsStory,
  type: "hn-story",
  key: "id",
});

export const hackerNewsResources: ReadonlyArray<StreamRef.StreamRef<unknown>> = [
  hackerNewsSource,
  hackerNewsTarget,
];
