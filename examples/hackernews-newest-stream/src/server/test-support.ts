/* oxlint-disable effecttsgo/async-function -- Bun tests own the ManagedRuntime lifetime. */
import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import type { CheckpointStore } from "@streamsy/projection";
import { type Array, Effect, Layer, ManagedRuntime, Stream } from "effect";
import type { HnStory } from "../state-schema.ts";
import { HackerNewsStories, storiesAddress } from "./stories.ts";
import { createStoriesLayer, host } from "./streams.ts";

export function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}

export async function demoHarness(readLimit = 1000) {
  const runtime = ManagedRuntime.make(
    createStoriesLayer.pipe(Layer.provideMerge(host("memory", { readLimit }))),
  );

  const context = await runtime.runPromise(Effect.context<CheckpointStore | HackerNewsStories>());

  return {
    runtime,
    context,
    clientLayer: Layer.succeedContext(context),
    close: () => runtime.dispose(),
    append: (
      changes: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof storiesAddress.collections>>,
    ) =>
      runtime.runPromise(
        Effect.flatMap(HackerNewsStories, (stories) =>
          stories.append(
            {},
            DurableState.changes(storiesAddress, { offset: ZERO_OFFSET }, changes),
          ),
        ),
      ),
    read: () =>
      runtime.runPromise(
        Stream.unwrap(Effect.map(HackerNewsStories, (stories) => stories.read({}))).pipe(
          Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
          Stream.runCollect,
        ),
      ),
  };
}
