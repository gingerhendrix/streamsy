import type { HnStory } from "../../state-schema.ts";
import { newestStorySort } from "../../state-schema.ts";
import {
  sourceDelete,
  sourceUpsert,
  type HackerNewsSourceChange,
} from "../story-index-projection.ts";

export type Reconciliation = {
  readonly nextStories: ReadonlyMap<number, HnStory>;
  readonly removed: readonly HnStory[];
  readonly changed: readonly HnStory[];
  readonly sourceChanges: readonly HackerNewsSourceChange[];
};

/** Pure deterministic newest-set reconciliation with unchanged-write suppression. */
export function reconcileNewest(
  previous: ReadonlyMap<number, HnStory>,
  newestIdSet: ReadonlySet<number>,
  fetched: readonly HnStory[],
): Reconciliation {
  const nextStories = new Map([...previous].filter(([id]) => newestIdSet.has(id)));
  for (const story of fetched) {
    if (newestIdSet.has(story.id)) nextStories.set(story.id, story);
  }
  const removed = [...previous.values()]
    .filter((story) => !nextStories.has(story.id))
    .toSorted((a, b) => a.id - b.id);
  const changed = [...nextStories.values()]
    .filter((story) => {
      const prior = previous.get(story.id);
      return prior === undefined || !storyEquals(prior, story);
    })
    .toSorted(newestStorySort);
  return {
    nextStories,
    removed,
    changed,
    sourceChanges: [...removed.map(sourceDelete), ...changed.map(sourceUpsert)],
  };
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
