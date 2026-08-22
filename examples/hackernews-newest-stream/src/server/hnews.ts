import { Effect, Schema } from "effect";
import { HackerNewsItem, HackerNewsStory, newestStorySort, type HnStory } from "../state-schema.ts";
import { hnApiBase } from "./config.ts";
import { pollFailure, type HackerNewsApi } from "./poller/contract.ts";

// Defaults to the public HN Firebase API. Override with HN_API_BASE to point the
// poller at a local fixture (used by the offline smoke test).
const defaultHnBase = hnApiBase;

const NewestStoryIds = Schema.Array(Schema.Finite);

// oxlint-disable-next-line effecttsgo/async-function -- This exported Promise helper is the documented HN compatibility adapter used at non-Effect edges.
export async function fetchNewestStoryIds(
  limit: number,
  apiBase = defaultHnBase,
): Promise<number[]> {
  // oxlint-disable-next-line effecttsgo/global-fetch -- This exported Promise compatibility adapter owns the Web fetch boundary and is wrapped by liveHackerNewsApi for Effect orchestration.
  const response = await fetch(`${apiBase}/newstories.json`);
  if (!response.ok)
    throw new Error(`HN newstories failed: ${response.status} ${response.statusText}`);
  const ids = Schema.decodeUnknownSync(NewestStoryIds)(await response.json());
  return ids.slice(0, limit);
}

// oxlint-disable-next-line effecttsgo/async-function -- This exported Promise helper is the documented HN compatibility adapter used at non-Effect edges.
export async function fetchStory(id: number, apiBase = defaultHnBase): Promise<HnStory | null> {
  // oxlint-disable-next-line effecttsgo/global-fetch -- This exported Promise compatibility adapter owns the Web fetch boundary and is wrapped by liveHackerNewsApi for Effect orchestration.
  const response = await fetch(`${apiBase}/item/${id}.json`);
  if (!response.ok)
    throw new Error(`HN item ${id} failed: ${response.status} ${response.statusText}`);
  const item = Schema.decodeUnknownSync(HackerNewsItem)(await response.json());
  if (!item || item.deleted || item.dead || item.type !== "story" || !item.title || !item.time)
    return null;

  return Schema.decodeUnknownSync(HackerNewsStory)({
    id: item.id,
    ...(item.by === undefined ? {} : { by: item.by }),
    ...(item.descendants === undefined ? {} : { descendants: item.descendants }),
    ...(item.score === undefined ? {} : { score: item.score }),
    time: item.time,
    title: item.title,
    type: "story",
    ...(item.url === undefined ? {} : { url: item.url }),
    ...(item.text === undefined ? {} : { text: item.text }),
  });
}

/**
 * Fetch story ids independently. Rejected or skipped items are logged and do not
 * stall the batch, so later polls can observe them again.
 */
// oxlint-disable-next-line effecttsgo/async-function -- This exported Promise batch helper preserves per-item Promise settlement and compatibility behavior.
export async function fetchStoriesById(
  ids: readonly number[],
  apiBase = defaultHnBase,
): Promise<HnStory[]> {
  const settled = await Promise.allSettled(ids.map((id) => fetchStory(id, apiBase)));
  const stories: HnStory[] = [];

  for (const result of settled) {
    if (result.status === "fulfilled" && result.value) stories.push(result.value);
    // oxlint-disable-next-line effecttsgo/global-console -- The Promise compatibility adapter reports skipped item failures at its terminal-facing boundary.
    if (result.status === "rejected") console.warn("Unable to fetch HN story", result.reason);
  }

  return stories.toSorted(newestStorySort);
}

/** Named Effect adapters over the Promise-based HN Firebase fetch helpers. */
export const liveHackerNewsApi: HackerNewsApi = {
  fetchNewestStoryIds: Effect.fn("HackerNewsApi.fetchNewestStoryIds")(function* (limit: number) {
    return yield* Effect.tryPromise({
      try: () => fetchNewestStoryIds(limit),
      catch: pollFailure("fetchNewestStoryIds"),
    });
  }),
  fetchStoriesById: Effect.fn("HackerNewsApi.fetchStoriesById")(function* (ids: readonly number[]) {
    return yield* Effect.tryPromise({
      try: () => fetchStoriesById(ids),
      catch: pollFailure("fetchStoriesById"),
    });
  }),
};
