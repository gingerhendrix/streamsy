import { Effect, Record, type Schedule } from "effect";
import { DurableState, type OffsetMismatch } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { newestStorySort, type HnStory } from "../../state-schema.ts";
import { applyChange, newestRows, type StoryRows } from "../newest-rows.ts";
import { storiesAddress, type StoryChange } from "../stories.ts";

export type FetchResult = {
  readonly newestIds: ReadonlySet<number>;
  readonly stories: readonly HnStory[];
};

/**
 * Read, decide, append. `Projection.decide` runs the rows to the tail, reads
 * them with their offset, and appends the diff with that offset as
 * `expectedOffset`. After `OffsetMismatch` it runs, reads and decides again
 * while `retry` allows.
 */
export const reconcileNewest = (
  fetched: FetchResult,
  retry: Schedule.Schedule<unknown, OffsetMismatch>,
) =>
  Projection.decide(newestRows, {
    input: "input",
    decide: ({ state: rows, offsets }) => {
      const changes = diffNewest(rows, fetched);
      // No change gives no events: `decide` then appends nothing and does not check the tail.
      const events = DurableState.changes(storiesAddress, { offset: offsets.input }, changes);

      return Effect.succeed({
        value: { rows: events.reduce(applyChange, rows), changes },
        items: events,
      });
    },
    retry,
  }).pipe(
    Effect.map(({ value, offset }) => ({ ...value, offset })),
    Effect.withSpan("NewestStoriesPoller.reconcile"),
  );

/** Keep known rows on item-fetch failures while their ids remain in the newest set. */
export function diffNewest(previous: StoryRows, fetched: FetchResult): readonly StoryChange[] {
  const removed = Record.values(previous)
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
