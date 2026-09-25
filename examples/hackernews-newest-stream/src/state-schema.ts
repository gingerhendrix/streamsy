import { createStateSchema } from "@durable-streams/state";
import { Schema } from "effect";

export const HackerNewsStory = Schema.Struct({
  id: Schema.Finite,
  by: Schema.optionalKey(Schema.String),
  descendants: Schema.optionalKey(Schema.Finite),
  score: Schema.optionalKey(Schema.Finite),
  time: Schema.Finite,
  title: Schema.String,
  type: Schema.Literal("story"),
  url: Schema.optionalKey(Schema.String),
  text: Schema.optionalKey(Schema.String),
});
export interface HnStory extends Schema.Schema.Type<typeof HackerNewsStory> {}

export const HackerNewsItem = Schema.NullOr(
  Schema.Struct({
    id: Schema.Finite,
    deleted: Schema.optionalKey(Schema.Boolean),
    dead: Schema.optionalKey(Schema.Boolean),
    type: Schema.optionalKey(Schema.String),
    by: Schema.optionalKey(Schema.String),
    time: Schema.optionalKey(Schema.Finite),
    text: Schema.optionalKey(Schema.String),
    kids: Schema.optionalKey(Schema.Array(Schema.Finite)),
    descendants: Schema.optionalKey(Schema.Finite),
    score: Schema.optionalKey(Schema.Finite),
    title: Schema.optionalKey(Schema.String),
    url: Schema.optionalKey(Schema.String),
  }),
);

export const ApiStatus = Schema.Struct({
  streamPath: Schema.String,
  newestLimit: Schema.Finite,
  pollIntervalMs: Schema.Finite,
  polling: Schema.Boolean,
  lastPollStartedAt: Schema.optional(Schema.String),
  lastPollCompletedAt: Schema.optional(Schema.String),
  lastPollError: Schema.optional(Schema.String),
  lastOffset: Schema.optional(Schema.String),
  lastStoryCount: Schema.Finite,
  lastUpserts: Schema.Finite,
  lastDeletes: Schema.Finite,
  totalConflictsRetried: Schema.Finite,
});
export interface ApiStatus extends Schema.Schema.Type<typeof ApiStatus> {}

export const ApiStatusSmokeView = ApiStatus;

export const hackerNewsState = createStateSchema({
  stories: {
    schema: Schema.toStandardSchemaV1(HackerNewsStory),
    type: "hn-story",
    primaryKey: "id",
  },
});

export function newestStorySort(a: HnStory, b: HnStory): number {
  return b.time - a.time || b.id - a.id;
}
