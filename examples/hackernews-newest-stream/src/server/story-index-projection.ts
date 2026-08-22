import * as StateProjection from "@streamsy/experimental/state-projection";
import { Schema } from "effect";
import {
  HackerNewsStateChange,
  HackerNewsStory,
  hackerNewsState,
  type HnStory,
} from "../state-schema.ts";

export const HackerNewsSourceChange = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("upsert"), story: HackerNewsStory }),
  Schema.Struct({
    operation: Schema.Literal("delete"),
    key: Schema.String,
    oldValue: HackerNewsStory,
  }),
]);

export type HackerNewsSourceChange = Schema.Schema.Type<typeof HackerNewsSourceChange>;
const encodeStateChange = Schema.encodeUnknownSync(HackerNewsStateChange);

export function sourceUpsert(story: HnStory): HackerNewsSourceChange {
  return { operation: "upsert", story };
}

export function sourceDelete(story: HnStory): HackerNewsSourceChange {
  return { operation: "delete", key: String(story.id), oldValue: story };
}

/**
 * Convert deterministic newest-set reconciliation commands into the public
 * Durable State vocabulary consumed by createStreamDB in the browser.
 */
export const hackerNewsStoryIndex = StateProjection.make({
  id: "hacker-news-newest-story-index",
  version: 1,
  input: HackerNewsSourceChange,
  project: ({ value, source, index }) => {
    const headers = {
      offset: source.position,
      txid: `${source.position}:${index}`,
    };

    if (value.operation === "delete") {
      return [
        encodeStateChange(
          hackerNewsState.stories.delete({
            key: value.key,
            oldValue: value.oldValue,
            headers,
          }),
        ),
      ];
    }

    return [encodeStateChange(hackerNewsState.stories.upsert({ value: value.story, headers }))];
  },
});
