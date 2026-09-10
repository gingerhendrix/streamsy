import { Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import { Context, Effect, Layer } from "effect";
import { streamPrefix } from "./config.ts";
import { pollFailure } from "./poller/contract.ts";
import { hackerNewsResources, hackerNewsSource } from "./stream-resources.ts";
import type { HackerNewsSourceChange } from "./story-index-projection.ts";

export { hackerNewsResources, hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
const appendSourceBatch = Effect.fn("DemoStreams.appendSourceBatch")(function* (
  items: readonly HackerNewsSourceChange[],
) {
  const result = yield* Streams.append(hackerNewsSource.ref, items).pipe(
    Effect.mapError(pollFailure("appendSourceBatch")),
  );
  return result.offset;
});
export class DemoStreams extends Context.Service<
  DemoStreams,
  {
    readonly fetch: (request: Request) => Promise<Response>;
    readonly appendSourceBatch: (
      items: readonly HackerNewsSourceChange[],
    ) => Effect.Effect<string, import("./poller/contract.ts").PollFailure>;
  }
>()("HackerNews/DemoStreams") {}

/** The HTTP conversion reuses the acquired services, never builds a second store. */
export const demoStreamsLayer = Layer.effect(
  DemoStreams,
  Effect.gen(function* () {
    const context = yield* Effect.context<StreamsReader | StreamsWriter>();
    for (const resource of hackerNewsResources) {
      yield* Streams.create(resource.ref).pipe(Effect.orDie);
    }
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() => Http.makeEdge({ pathPrefix: streamPrefix }, Layer.succeedContext(context))),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );
    return DemoStreams.of({
      fetch: (request) => edge.handler(request),
      appendSourceBatch: (items) => appendSourceBatch(items).pipe(Effect.provide(context)),
    });
  }),
);
export const demoMemoryLayer = demoStreamsLayer.pipe(Layer.provideMerge(Streams.layerMemory()));
