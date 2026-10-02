import { Memory, Streamsy } from "@streamsy/core";
import { CheckpointStore } from "@streamsy/projection";
import * as BunStorage from "@streamsy/storage/bun";
import { Effect, Layer } from "effect";
import { DemoConfig } from "./config.ts";
import { HackerNewsStories } from "./stories.ts";

const streams = Streamsy.group(HackerNewsStories);

/** Create before HTTP starts, so a fresh State replay is 200 and empty. */
export const createStoriesLayer = Layer.effectDiscard(
  Effect.flatMap(HackerNewsStories, (stories) => stories.create({})),
);

const storage = (databasePath: string, options: { readonly readLimit?: number } = {}) =>
  databasePath === "memory"
    ? Memory.layer(options, streams)
    : BunStorage.layer({ ...options, client: { filename: databasePath } }, streams);

/** The checkpoints join the owner of the stream: one SQLite file, or memory. */
export const host = (databasePath: string, options: { readonly readLimit?: number } = {}) =>
  CheckpointStore.layer.pipe(Layer.provideMerge(storage(databasePath, options)));

const hostLayer = Layer.unwrap(Effect.map(DemoConfig, ({ databasePath }) => host(databasePath)));

/** One SQLite file owns the stream, checkpoint and fold state. */
export const demoHostLayer = createStoriesLayer.pipe(Layer.provideMerge(hostLayer));
