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

  // TanStack DB's orderBy/limit optimization looks up an index for the first
  // ordering field. The query still uses id as its deterministic tie-breaker,
  // while this scalar time index avoids loading the entire collection.
  storiesCollection.createIndex((story) => story.time, { indexType: BasicIndex });

  return { storiesCollection, storiesWriter };
}
