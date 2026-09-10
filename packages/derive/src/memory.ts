import { Effect, Layer, Schema } from "effect";
import { Streams } from "@streamsy/core";
import { MemoryCommitBoundary } from "@streamsy/core/storage";
import { Commit } from "./commit.ts";
import { Checkpoint, StateRecord, records } from "./stores.ts";

/** Requires the exact memory owner retained by Streams.layerMemory(). */
export const layer = Layer.effect(
  Commit,
  Effect.gen(function* () {
    const boundary = yield* MemoryCommitBoundary;
    return Commit.of({
      withTransaction: boundary.withTransaction,
      checkpoints: records(Schema.fromJsonString(Checkpoint), "checkpoint", boundary),
      states: records(Schema.fromJsonString(StateRecord), "state", boundary),
    });
  }),
);

/** A single graph retains the real reader, writer and fused memory owner. */
export const layerMemory = (options: Parameters<typeof Streams.layerMemory>[0] = {}) =>
  layer.pipe(Layer.provideMerge(Streams.layerMemory(options)));
