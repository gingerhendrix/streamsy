import { HackerNewsSource, HackerNewsTarget } from "./stream-resources.ts";
import type { Checkpoints, State } from "@streamsy/projection";
import { Effect, Layer, ManagedRuntime, Stream } from "effect";
import type { HnStory } from "../state-schema.ts";
import { DemoStreams, demoStreamsLayer, memoryHost } from "./streams.ts";
export function story(id: number, time: number, title: string): HnStory {
  return { id, time, title, type: "story" };
}
/** One retained memory owner per harness: streams, checkpoints and the HTTP edge share it. */
export async function demoHarness(readLimit = 1000) {
  const runtime = ManagedRuntime.make(
    demoStreamsLayer.pipe(Layer.provideMerge(memoryHost({ readLimit }))),
  );
  const streams = await runtime.runPromise(DemoStreams);
  const context = await runtime.runPromise(
    Effect.context<Checkpoints | HackerNewsSource | HackerNewsTarget | State>(),
  );
  return {
    runtime,
    streams,
    clientLayer: Layer.succeedContext(context),
    close: () => runtime.dispose(),
    append: (
      streamId: string,
      items: readonly import("./source-change.ts").HackerNewsSourceChange[],
    ) =>
      runtime.runPromise(
        Effect.flatMap(HackerNewsSource, (source) => {
          if (!HackerNewsSource.match(streamId))
            return Effect.die(new RangeError("Unknown source"));
          return source.append({}, items);
        }),
      ),
    read: (streamId: string): Promise<ReadonlyArray<unknown>> => {
      if (HackerNewsSource.match(streamId))
        return runtime.runPromise(
          Effect.flatMap(HackerNewsSource, (source) =>
            source.read({}).pipe(
              Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
              Stream.runCollect,
            ),
          ),
        );
      return runtime.runPromise(
        Effect.flatMap(HackerNewsTarget, (target) => {
          if (!HackerNewsTarget.match(streamId))
            return Effect.die(new RangeError("Unknown target"));
          return target.read({}).pipe(
            Stream.flatMap((batch) => Stream.fromIterable(batch.items)),
            Stream.runCollect,
          );
        }),
      );
    },
  };
}
