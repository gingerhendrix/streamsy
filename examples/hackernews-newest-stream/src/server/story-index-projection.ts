import type { JsonValue } from "@streamsy/core";
import { StateProjection } from "@streamsy/experimental/effect/state-projection";
import { Schema } from "effect";

export const HackerNewsStory = Schema.Struct({
  id: Schema.Number,
  by: Schema.optionalKey(Schema.String),
  descendants: Schema.optionalKey(Schema.Number),
  score: Schema.optionalKey(Schema.Number),
  time: Schema.Number,
  title: Schema.String,
  type: Schema.Literal("story"),
  url: Schema.optionalKey(Schema.String),
  text: Schema.optionalKey(Schema.String),
});

/**
 * First experimental consumer seam for the bounded newest-story view.
 *
 * The stable story id is the Durable State key. The scalar `time` value stays
 * in each row so consumers can maintain the bounded newest-story index.
 */
export const hackerNewsStoryIndex = StateProjection.make({
  id: "hacker-news-newest-story-index",
  version: 1,
  input: HackerNewsStory,
  project: ({ value, source, index }) => [
    {
      type: "hn-story",
      key: String(value.id),
      value,
      headers: {
        operation: "upsert",
        offset: source.position,
        txid: `${source.position}:${index}`,
      },
    } satisfies JsonValue,
  ],
});
