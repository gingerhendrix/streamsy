import { createMemoryStorageAdapter, type StorageAdapter } from "@streamsy/core";
import { StateProjection } from "@streamsy/experimental/effect/state-projection";
import type { HnStory } from "../state-schema.ts";
import { makeStoryProjectionInstance } from "./projection.ts";
import { DemoStreams } from "./streams.ts";

export function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}

export async function demoHarness(adapter: StorageAdapter = createMemoryStorageAdapter()) {
  const streams = new DemoStreams(adapter);
  await streams.start();
  return {
    adapter,
    streams,
    client: streams.client,
    clientLayer: StateProjection.layerClient(streams.client),
    projection: makeStoryProjectionInstance(),
  };
}
