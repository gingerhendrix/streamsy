import { Streamsy, type DurableState, type StreamAddress } from "@streamsy/core";
import { HackerNewsStory } from "../state-schema.ts";

export class HackerNewsStories extends Streamsy.state<HackerNewsStories>()(
  "HackerNews/Stories",
  "hackernews/newest",
  { params: {}, collections: { "hn-story": { schema: HackerNewsStory, key: "id" } } },
) {}

export const storiesAddress = HackerNewsStories.at({});
export type StoryChange = DurableState.Change<typeof storiesAddress.collections>;
export type StoryEvent = StreamAddress.CollectionsChange<typeof storiesAddress.collections>;
