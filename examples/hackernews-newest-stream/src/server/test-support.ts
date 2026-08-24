import { createMemoryStorageAdapter, type StorageAdapter } from "@streamsy/core";
import * as StateProjection from "@streamsy/experimental/state-projection";
import { ManagedRuntime } from "effect";
import type { HnStory } from "../state-schema.ts";
import { StoryProjectionInstance, storyProjectionInstanceLayer } from "./projection.ts";
import { DemoStreams } from "./streams.ts";

export function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}

// oxlint-disable-next-line effecttsgo/async-function -- This test helper assembles the Promise-native protocol harness consumed by Vitest.
export async function demoHarness(adapter: StorageAdapter = createMemoryStorageAdapter()) {
  const streams = new DemoStreams(adapter);
  await streams.start();
  const projectionRuntime = ManagedRuntime.make(storyProjectionInstanceLayer);
  const projection = projectionRuntime.runSync(StoryProjectionInstance);
  await projectionRuntime.dispose();
  return {
    adapter,
    streams,
    client: streams.client,
    clientLayer: StateProjection.layerClient(streams.client),
    projection,
  };
}
