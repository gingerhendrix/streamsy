import type { HnStory } from "../state-schema.ts";

// Defaults to the public HN Firebase API. Override with HN_API_BASE to point the
// poller at a local fixture (used by the offline smoke test).
const hnBase = (process.env.HN_API_BASE ?? "https://hacker-news.firebaseio.com/v0").replace(
  /\/$/,
  "",
);

type HnItem = {
  id: number;
  deleted?: boolean;
  dead?: boolean;
  type?: string;
  by?: string;
  time?: number;
  text?: string;
  kids?: number[];
  descendants?: number;
  score?: number;
  title?: string;
  url?: string;
};

export async function fetchNewestStoryIds(limit: number): Promise<number[]> {
  const response = await fetch(`${hnBase}/newstories.json`);
  if (!response.ok)
    throw new Error(`HN newstories failed: ${response.status} ${response.statusText}`);
  const ids = (await response.json()) as number[];
  return ids.slice(0, limit);
}

export async function fetchStory(id: number): Promise<HnStory | null> {
  const response = await fetch(`${hnBase}/item/${id}.json`);
  if (!response.ok)
    throw new Error(`HN item ${id} failed: ${response.status} ${response.statusText}`);
  const item = (await response.json()) as HnItem | null;
  if (!item || item.deleted || item.dead || item.type !== "story" || !item.title || !item.time)
    return null;

  return {
    id: item.id,
    by: item.by,
    descendants: item.descendants,
    score: item.score,
    time: item.time,
    title: item.title,
    type: "story",
    url: item.url,
    text: item.text,
  };
}

export async function fetchStoriesById(ids: readonly number[]): Promise<HnStory[]> {
  const settled = await Promise.allSettled(ids.map((id) => fetchStory(id)));
  const stories: HnStory[] = [];

  for (const result of settled) {
    if (result.status === "fulfilled" && result.value) stories.push(result.value);
    if (result.status === "rejected") console.warn("Unable to fetch HN story", result.reason);
  }

  return stories.toSorted(newestStorySort);
}

export function newestStorySort(a: HnStory, b: HnStory): number {
  return b.time - a.time || b.id - a.id;
}
