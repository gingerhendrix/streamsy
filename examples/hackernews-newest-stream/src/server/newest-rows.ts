import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { HackerNewsStory } from "../state-schema.ts";
import { storiesAddress, type StoryEvent } from "./stories.ts";

export const StoryRows = Schema.Record(Schema.String, HackerNewsStory);
export type StoryRows = Schema.Schema.Type<typeof StoryRows>;

export function applyChange(rows: StoryRows, event: StoryEvent): StoryRows {
  const next = { ...rows };
  if (event.headers.operation === "delete") delete next[event.key];
  else if ("value" in event) next[event.key] = event.value;
  return next;
}

/** The small, bounded newest set and its checkpoint commit in one transaction. */
export const newestRows = Projection.fold({
  id: "hn-newest-rows",
  params: {},
  inputs: { input: storiesAddress },
  state: { schema: StoryRows, initial: {} },
  fold: (rows, { item }) => applyChange(rows, item),
}).at({});
