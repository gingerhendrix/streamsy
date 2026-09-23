import { Schema } from "effect";
import { HackerNewsStory, type HnStory } from "../state-schema.ts";

/** Deterministic newest-set reconciliation commands appended to the source stream. */
export const HackerNewsSourceChange = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("upsert"), story: HackerNewsStory }),
  Schema.Struct({
    operation: Schema.Literal("delete"),
    key: Schema.String,
  }),
]);

export type HackerNewsSourceChange = Schema.Schema.Type<typeof HackerNewsSourceChange>;

export function sourceUpsert(story: HnStory): HackerNewsSourceChange {
  return { operation: "upsert", story };
}

export function sourceDelete(story: HnStory): HackerNewsSourceChange {
  return { operation: "delete", key: String(story.id) };
}
