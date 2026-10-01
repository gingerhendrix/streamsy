import { Array, Effect } from "effect";
import { DurableState } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { newestStorySort, type HnStory } from "../../state-schema.ts";
import { applyChange, newestRows, type StoryRows } from "../newest-rows.ts";
import { HackerNewsStories, storiesAddress, type StoryChange } from "../stories.ts";

export type FetchResult = {
  readonly newestIds: ReadonlySet<number>;
  readonly stories: readonly HnStory[];
};

/**
 * One pass of read, decide, append. Fails with `OffsetMismatch` when another
 * writer appended after the read; the caller retries the whole pass.
 */
export const reconcileNewest = Effect.fn("NewestStoriesPoller.reconcile")(function* (
  fetched: FetchResult,
) {
  // Run to the tail. A run with a limit would leave the offset behind the tail.
  yield* Projection.run(newestRows);
  // The rows and the offset they are at, from one transaction.
  const { state: rows, offsets } = yield* Projection.read(newestRows);
  const changes = diffNewest(rows, fetched);
  // An append needs items. No change appends nothing, so it does not check the tail.
  if (!Array.isReadonlyArrayNonEmpty(changes)) return { rows, changes, offset: offsets.input };
  const events = DurableState.changes(storiesAddress, { offset: offsets.input }, changes);
  const stories = yield* HackerNewsStories;
  const appended = yield* stories.append({}, events, { expectedOffset: offsets.input });
  return { rows: events.reduce(applyChange, rows), changes, offset: appended.offset };
});

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
    ...removed.map((story) => DurableState.remove("hn-story", story.id, { oldValue: story })),
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
