import type { JsonValue } from "@streamsy/core";
import { StateProjection } from "@streamsy/experimental/effect/state-projection";
import { Schema } from "effect";
import { hackerNewsState, type HnStory } from "../state-schema.ts";

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

type EffectStory = Schema.Schema.Type<typeof HackerNewsStory>;

// Keep this Effect Schema aligned with the zod schema in state-schema.ts.
const _zodAcceptsEffectStory: HnStory = {} as EffectStory;
const _effectAcceptsZodStory: EffectStory = {} as HnStory;
void _zodAcceptsEffectStory;
void _effectAcceptsZodStory;

export const HackerNewsSourceChange = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("upsert"), story: HackerNewsStory }),
  Schema.Struct({
    operation: Schema.Literal("delete"),
    key: Schema.String,
    oldValue: HackerNewsStory,
  }),
]);

export type HackerNewsSourceChange = Schema.Schema.Type<typeof HackerNewsSourceChange>;

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
        toJson(
          hackerNewsState.stories.delete({
            key: value.key,
            oldValue: value.oldValue,
            headers,
          }),
        ),
      ];
    }

    return [toJson(hackerNewsState.stories.upsert({ value: value.story, headers }))];
  },
});

function toJson(value: unknown): JsonValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (Array.isArray(value)) return value.map(toJson);
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter((entry) => entry[1] !== undefined)
        .map(([key, entryValue]) => [key, toJson(entryValue)]),
    );
  }
  throw new TypeError(`Durable State values must be JSON; received ${typeof value}`);
}
