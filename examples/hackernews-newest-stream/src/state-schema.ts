import { createStateSchema, type ChangeEvent } from "@durable-streams/state";
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

const StateHeaders = Schema.Struct({
  operation: Schema.Literals(["upsert", "delete"]),
  offset: Schema.String,
  txid: Schema.String,
});

export const HackerNewsStateChange = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("hn-story"),
    key: Schema.String,
    value: HackerNewsStory,
    headers: StateHeaders,
  }),
  Schema.Struct({
    type: Schema.Literal("hn-story"),
    key: Schema.String,
    old_value: Schema.optionalKey(HackerNewsStory),
    headers: StateHeaders,
  }),
]);
export type HackerNewsStateChange = Schema.Schema.Type<typeof HackerNewsStateChange>;

export const ApiStatus = Schema.Struct({
  streamPath: Schema.String,
  sourceStreamPath: Schema.String,
  newestLimit: Schema.Finite,
  pollIntervalMs: Schema.Finite,
  polling: Schema.Boolean,
  projection: Schema.Struct({
    running: Schema.Boolean,
    lastError: Schema.optional(Schema.String),
    sourceThrough: Schema.optionalKey(Schema.String),
  }),
  lastPollStartedAt: Schema.optional(Schema.String),
  lastPollCompletedAt: Schema.optional(Schema.String),
  lastPollError: Schema.optional(Schema.String),
  lastSourceOffset: Schema.optional(Schema.String),
  lastStoryCount: Schema.Finite,
  lastFetchedNewStories: Schema.Finite,
  lastRefreshedStories: Schema.Finite,
  lastChangedStories: Schema.Finite,
  lastRemovedStories: Schema.Finite,
  sourceBatches: Schema.Finite,
  sourceChanges: Schema.Finite,
});
export interface ApiStatus extends Schema.Schema.Type<typeof ApiStatus> {}

export const ApiStatusSmokeView = Schema.Struct({
  lastPollCompletedAt: ApiStatus.fields.lastPollCompletedAt,
  lastPollError: ApiStatus.fields.lastPollError,
  lastSourceOffset: ApiStatus.fields.lastSourceOffset,
  lastStoryCount: ApiStatus.fields.lastStoryCount,
  sourceBatches: ApiStatus.fields.sourceBatches,
  sourceChanges: ApiStatus.fields.sourceChanges,
  projection: ApiStatus.fields.projection,
});

export const hackerNewsState = createStateSchema({
  stories: {
    schema: Schema.toStandardSchemaV1(HackerNewsStory),
    type: "hn-story",
    primaryKey: "id",
  },
});

export type HnStateEvent = ChangeEvent<HnStory>;

export function newestStorySort(a: HnStory, b: HnStory): number {
  return b.time - a.time || b.id - a.id;
}
