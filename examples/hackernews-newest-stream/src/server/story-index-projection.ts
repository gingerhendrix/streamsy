import { Output, Projection } from "@streamsy/projection";
import { Effect } from "effect";
import { HackerNewsStory } from "../state-schema.ts";
import { hackerNewsSource, HackerNewsTarget } from "./stream-resources.ts";

/** The output name is the browser's Durable State collection type. */
export const hackerNewsStoryIndex = Projection.outputs({
  id: "hn-story-index",
  generation: 1,
  inputs: { input: hackerNewsSource },
  outputs: {
    "hn-story": Output.rows(HackerNewsStory, { key: "id", stream: HackerNewsTarget.at({}) }),
  },
  process: (batch) =>
    Effect.succeed({
      "hn-story": Projection.items(batch).map(({ item }) =>
        item.operation === "delete" ? Output.remove(item.key) : Output.upsert(item.story),
      ),
    }),
});

export const hackerNewsTarget = hackerNewsStoryIndex.outputs["hn-story"].ref({});
