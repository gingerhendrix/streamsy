import * as ProjectionMemory from "@streamsy/projection/memory";
import { Streams, StreamRef } from "@streamsy/core";
import type { Host, State } from "@streamsy/projection";
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "effect";
import type { HnStory } from "../state-schema.ts";
import { DemoStreams, demoStreamsLayer } from "./streams.ts";
export function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}
/** One retained memory owner per harness: streams, checkpoints and the HTTP edge share it. */
export async function demoHarness(readLimit = 1000) {
  const runtime = ManagedRuntime.make(
    demoStreamsLayer.pipe(Layer.provideMerge(ProjectionMemory.layerMemory({ readLimit }))),
  );
  const streams = await runtime.runPromise(DemoStreams);
  const context = await runtime.runPromise(Effect.context<Host | State>());
  return {
    runtime,
    streams,
    clientLayer: Layer.succeedContext(context),
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
