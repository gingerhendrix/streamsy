/* oxlint-disable effecttsgo/async-function -- Bun test harness owns the runtime edge. */
import { Streams, StreamRef } from "@streamsy/core-next";
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "effect";
import type { HnStory } from "../state-schema.ts";
import { StoryProjectionInstance, storyProjectionInstanceLayer } from "./projection.ts";
import { DemoStreams, demoMemoryLayer } from "./streams.ts";
export function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}
export async function demoHarness() {
  const runtime = ManagedRuntime.make(Layer.merge(demoMemoryLayer, storyProjectionInstanceLayer));
  const streams = await runtime.runPromise(DemoStreams);
  const context = await runtime.runPromise(
    Effect.context<
      import("@streamsy/core-next").StreamsReader | import("@streamsy/core-next").StreamsWriter
    >(),
  );
  return {
    runtime,
    streams,
    clientLayer: Layer.succeedContext(context),
    projection: await runtime.runPromise(StoryProjectionInstance),
    close: () => runtime.dispose(),
    append: (streamId: string, items: readonly unknown[]) =>
      runtime.runPromise(
        Streams.append(StreamRef.json(streamId, { schema: Schema.Unknown }), items),
      ),
    read: (streamId: string) =>
      runtime.runPromise(
        Streams.read(StreamRef.json(streamId, { schema: Schema.Unknown })).pipe(
          Streams.items,
          Stream.runCollect,
        ),
      ),
  };
}
