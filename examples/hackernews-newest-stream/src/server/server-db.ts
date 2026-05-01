import { BasicIndex, type Collection } from "@tanstack/db";
import type { HnStory } from "../state-schema.ts";
import { createServerCollection, type ServerCollectionWriter } from "./server-collection.ts";

export type HnServerDb = {
  storiesCollection: Collection<HnStory, number>;
  storiesWriter: ServerCollectionWriter<HnStory, number>;
};

export function createHnServerDb(): HnServerDb {
  const { collection: storiesCollection, writer: storiesWriter } = createServerCollection<HnStory, number>({
    id: "hn-stories",
    getKey: (story) => story.id,
  });

  // The server projection orders and limits by rank. An explicit index keeps
  // TanStack DB from falling back to a full lazy load path for that query.
  storiesCollection.createIndex((story) => story.rank, { indexType: BasicIndex });

  return { storiesCollection, storiesWriter };
}
