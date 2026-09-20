/* oxlint-disable effecttsgo/node-builtin-import -- This executable demo prepares its configured SQLite directory at the Bun edge. */
import { Storage, StorageFault, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import { ProjectionFault, type Host } from "@streamsy/projection";
import * as ProjectionMemory from "@streamsy/projection/memory";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Layer } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { databasePath, streamPrefix } from "./config.ts";
import { pollFailure } from "./poller/contract.ts";
import { hackerNewsResources, hackerNewsSource } from "./stream-resources.ts";
import type { HackerNewsSourceChange } from "./source-change.ts";

export { hackerNewsResources, hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
const appendSourceBatch = Effect.fn("DemoStreams.appendSourceBatch")(function* (
  items: readonly HackerNewsSourceChange[],
) {
  const result = yield* Streams.append(hackerNewsSource, items).pipe(
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
    for (const ref of hackerNewsResources) {
      yield* Streams.create(ref).pipe(Effect.orDie);
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
const sqliteHostLayer = Layer.unwrap(
  Effect.promise(() => mkdir(dirname(databasePath), { recursive: true })).pipe(
    Effect.as(
      ProjectionSqlite.layer.pipe(
        Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: databasePath } })),
      ),
    ),
  ),
);

const memoryHost = Layer.effectContext(Effect.context<Host | Storage>()).pipe(
  Layer.provide(ProjectionMemory.layerMemory()),
);
const sqliteHost = Layer.effectContext(Effect.context<Host | Storage>()).pipe(
  Layer.provide(sqliteHostLayer),
);
const hostLayer: Layer.Layer<Host | Storage, ProjectionFault | StorageFault> =
  databasePath === "memory" ? memoryHost : sqliteHost;

/** One retained host graph shared by streams, checkpoints, HTTP and background fibers. */
export const demoHostLayer = demoStreamsLayer.pipe(Layer.provideMerge(hostLayer));
