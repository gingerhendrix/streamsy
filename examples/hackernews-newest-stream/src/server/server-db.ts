import { BasicIndex, type Collection } from "@tanstack/db";
import type { HnStory } from "../state-schema.ts";
import { createServerCollection, type ServerCollectionWriter } from "./server-collection.ts";

export type HnServerDb = {
  storiesCollection: Collection<HnStory, number>;
  storiesWriter: ServerCollectionWriter<HnStory, number>;
};

export function createHnServerDb(): HnServerDb {
  const { collection: storiesCollection, writer: storiesWriter } = createServerCollection<
    HnStory,
    number
  >({
    id: "hn-stories",
    getKey: (story) => story.id,
  });

  // The server projection orders and limits by HN creation time with id as a
  // deterministic tie-breaker. An explicit index keeps TanStack DB from falling
  // back to a full lazy load path.
  storiesCollection.createIndex((story) => [story.time, story.id], { indexType: BasicIndex });

  return { storiesCollection, storiesWriter };
}
