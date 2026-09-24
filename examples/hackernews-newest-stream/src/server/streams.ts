/* oxlint-disable effecttsgo/node-builtin-import -- This executable demo prepares its configured SQLite directory at the Bun edge. */
import { Memory, StreamGroup, type Storage, type StorageFault, type Changes } from "@streamsy/core";
import { ProjectionFault, type State, type Checkpoints } from "@streamsy/projection";
import * as ProjectionMemory from "@streamsy/projection/memory";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Layer } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { databasePath } from "./config.ts";
import { pollFailure } from "./poller/contract.ts";
import { HackerNewsSource, HackerNewsTarget } from "./stream-resources.ts";
import type { HackerNewsSourceChange } from "./source-change.ts";

export const streams = StreamGroup.make(HackerNewsSource, HackerNewsTarget);

const appendSourceBatch = Effect.fn("DemoStreams.appendSourceBatch")(function* (
  items: readonly HackerNewsSourceChange[],
) {
  const result = yield* (yield* HackerNewsSource)
    .append({}, items)
    .pipe(Effect.mapError(pollFailure("appendSourceBatch")));
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
    const context = yield* Effect.context<HackerNewsSource>();
    yield* (yield* HackerNewsSource).create({}).pipe(Effect.orDie);
    return DemoStreams.of({
      appendSourceBatch: (items) => appendSourceBatch(items).pipe(Effect.provide(context)),
    });
  }),
);
const sqliteHostLayer = Layer.unwrap(
  Effect.promise(() => mkdir(dirname(databasePath), { recursive: true })).pipe(
    Effect.as(
      ProjectionSqlite.layer.pipe(
        Layer.provideMerge(BunStorage.layer({ client: { filename: databasePath } }, streams)),
      ),
    ),
  ),
);

export const memoryHost = (
  options: Parameters<typeof Memory.layer>[0] & { readonly readLimit?: number } = {},
) => ProjectionMemory.layer.pipe(Layer.provideMerge(Memory.layer(options, streams)));
const hostLayer: Layer.Layer<
  | Checkpoints
  | State
  | Storage
  | HackerNewsSource
  | HackerNewsTarget
  | Changes<HackerNewsSource | HackerNewsTarget>,
  ProjectionFault | StorageFault
> = databasePath === "memory" ? memoryHost() : sqliteHostLayer;

/** One retained host graph shared by streams, checkpoints, HTTP and background fibers. */
export const demoHostLayer = demoStreamsLayer.pipe(Layer.provideMerge(hostLayer));
