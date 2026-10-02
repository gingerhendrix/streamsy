import { Context, Effect, Layer } from "effect";
import { CommitBoundary } from "@streamsy/core";
import { Streams } from "@streamsy/core/protocol";
import { OwnerRecords, ownerOf, attachOwner } from "@streamsy/core/internal";
import { CheckpointStore, fromStore, type EncodedStore } from "./checkpoint-store.ts";

/** Requires the exact memory owner retained by Streams.layerMemory(). */
export const layer = Layer.effectContext(
  Effect.gen(function* () {
    const boundary = yield* CommitBoundary;
    const records = yield* OwnerRecords;
    const store: EncodedStore = {
      read: (key) => Effect.orDie(records.read(key)),
      write: (key, value) => Effect.orDie(records.write(key, value)),
      remove: (key) => Effect.orDie(records.remove(key)),
      withTransaction: (body) =>
        Effect.catchTag(boundary.withTransaction(body), "SqlError", Effect.die),
    };
    const checkpoints = fromStore(store);
    attachOwner(checkpoints.load, ownerOf(boundary));
    return Context.make(CheckpointStore, checkpoints);
  }),
);

/** A single graph retains the real reader, writer and atomic memory owner. */
export const layerMemory = (options: Parameters<typeof Streams.layerMemory>[0] = {}) =>
  layer.pipe(Layer.provideMerge(Streams.layerMemory(options)));
