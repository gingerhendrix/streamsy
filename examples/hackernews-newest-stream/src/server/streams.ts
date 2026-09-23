/* oxlint-disable effecttsgo/node-builtin-import -- This executable demo prepares its configured SQLite directory at the Bun edge. */
import { Storage, StorageFault, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import { ProjectionFault, type State, type Host } from "@streamsy/projection";
import * as ProjectionMemory from "@streamsy/projection/memory";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Layer } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { databasePath } from "./config.ts";
import { pollFailure } from "./poller/contract.ts";
import { hackerNewsSource } from "./stream-resources.ts";
import type { HackerNewsSourceChange } from "./source-change.ts";

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
    readonly appendSourceBatch: (
      items: readonly HackerNewsSourceChange[],
    ) => Effect.Effect<string, import("./poller/contract.ts").PollFailure>;
  }
>()("HackerNews/DemoStreams") {}

/** Capture the shared host once for poller writes. */
export const demoStreamsLayer = Layer.effect(
  DemoStreams,
  Effect.gen(function* () {
    const context = yield* Effect.context<StreamsReader | StreamsWriter>();
    yield* Streams.create(hackerNewsSource).pipe(Effect.orDie);
    return DemoStreams.of({
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

const memoryHost = Layer.effectContext(Effect.context<Host | State | Storage>()).pipe(
  Layer.provide(ProjectionMemory.layerMemory()),
);
const sqliteHost = Layer.effectContext(Effect.context<Host | State | Storage>()).pipe(
  Layer.provide(sqliteHostLayer),
);
const hostLayer: Layer.Layer<Host | State | Storage, ProjectionFault | StorageFault> =
  databasePath === "memory" ? memoryHost : sqliteHost;

/** One retained host graph shared by streams, checkpoints, HTTP and background fibers. */
export const demoHostLayer = demoStreamsLayer.pipe(Layer.provideMerge(hostLayer));
