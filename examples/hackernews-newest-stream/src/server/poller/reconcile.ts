import { DurableState } from "@streamsy/core";
import { newestStorySort, type HnStory } from "../../state-schema.ts";
import type { StoryRows } from "../newest-rows.ts";
import type { StoryChange } from "../stories.ts";

export type FetchResult = {
  readonly newestIds: ReadonlySet<number>;
  readonly stories: readonly HnStory[];
};

/** Keep known rows on item-fetch failures while their ids remain in the newest set. */
export function diffNewest(previous: StoryRows, fetched: FetchResult): readonly StoryChange[] {
  const removed = Object.values(previous)
    .filter((story) => !fetched.newestIds.has(story.id))
    .toSorted((a, b) => a.id - b.id);
  const changed = fetched.stories
    .filter(
      (story) =>
        fetched.newestIds.has(story.id) &&
        (previous[String(story.id)] === undefined ||
          !storyEquals(previous[String(story.id)], story)),
    )
    .toSorted(newestStorySort);
  return [
    ...removed.map((story) => DurableState.remove("hn-story", story)),
    ...changed.map((story) => DurableState.upsert("hn-story", story)),
  ];
}

function storyEquals(a: HnStory, b: HnStory): boolean {
  return (
    a.id === b.id &&
    a.by === b.by &&
    a.descendants === b.descendants &&
    a.score === b.score &&
    a.time === b.time &&
    a.title === b.title &&
    a.type === b.type &&
    a.url === b.url &&
    a.text === b.text
  );
}
