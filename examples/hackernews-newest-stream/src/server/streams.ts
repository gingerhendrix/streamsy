/* oxlint-disable effecttsgo/node-builtin-import -- This executable demo prepares its configured SQLite directory at the Bun edge. */
import { Memory, Streamsy, type Storage, type StorageFault } from "@streamsy/core";
import type { CheckpointStore, ProjectionFault } from "@streamsy/projection";
import * as ProjectionMemory from "@streamsy/projection/memory";
import * as ProjectionSql from "@streamsy/projection/sql";
import * as BunStorage from "@streamsy/storage/bun";
import { Effect, Layer } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { databasePath } from "./config.ts";
import { HackerNewsStories } from "./stories.ts";

const streams = Streamsy.group(HackerNewsStories);

/** Create before HTTP starts, so a fresh State replay is 200 and empty. */
export const createStoriesLayer = Layer.effectDiscard(
  Effect.flatMap(HackerNewsStories, (stories) => stories.create({})),
);

const sqliteHostLayer = Layer.unwrap(
  Effect.promise(() => mkdir(dirname(databasePath), { recursive: true })).pipe(
    Effect.as(
      ProjectionSql.layerStorage.pipe(
        Layer.provideMerge(BunStorage.layer({ client: { filename: databasePath } }, streams)),
      ),
    ),
  ),
);

export const memoryHost = (
  options: Parameters<typeof Memory.layer>[0] & { readonly readLimit?: number } = {},
) => ProjectionMemory.layer.pipe(Layer.provideMerge(Memory.layer(options, streams)));
const hostLayer: Layer.Layer<
  CheckpointStore | Storage | HackerNewsStories | Streamsy.ChangeFeed<HackerNewsStories>,
  ProjectionFault | StorageFault
> = databasePath === "memory" ? memoryHost() : sqliteHostLayer;

/** One SQLite file owns the stream, checkpoint and fold state. */
export const demoHostLayer = createStoriesLayer.pipe(Layer.provideMerge(hostLayer));
