import { createEffect, eq, type Collection, type DeltaEvent, type Effect } from "@tanstack/db";
import { hackerNewsState, type HnStory } from "../state-schema.ts";

type StreamWriter = { appendJson(value: unknown): Promise<void | string> };

export function startNewestProjection(config: {
  storiesCollection: Collection<HnStory, number>;
  streams: StreamWriter;
}): Effect {
  return createEffect<HnStory, number>({
    id: "hn-newest-50-to-durable-state",
    query: (q) =>
      q
        .from({ story: config.storiesCollection })
        .where(({ story }) => eq(story.type, "story"))
        .orderBy(({ story }) => [story.time, story.id], "desc")
        .limit(50)
        .select(({ story }) => ({
          id: story.id,
          by: story.by,
          descendants: story.descendants,
          score: story.score,
          time: story.time,
          title: story.title,
          type: story.type,
          url: story.url,
          text: story.text,
        })),
    skipInitial: false,
    onBatch: async (events) => {
      for (const event of events) await appendHnDelta(config.streams, event);
    },
    onError: (error, event) => {
      console.error("HN projection failed", { error, event });
    },
    onSourceError: (error) => {
      console.error("HN source collection failed; projection disposed", error);
    },
  });
}

async function appendHnDelta(
  streams: StreamWriter,
  event: DeltaEvent<HnStory, number>,
): Promise<void> {
  const headers = {
    timestamp: new Date().toISOString(),
    txid: crypto.randomUUID(),
  };

  if (event.type === "exit") {
    await streams.appendJson(
      hackerNewsState.stories.delete({
        key: String(event.key),
        oldValue: event.value,
        headers,
      }),
    );
    return;
  }

  await streams.appendJson(
    hackerNewsState.stories.upsert({
      key: String(event.key),
      value: event.value,
      headers,
    }),
  );
}
