import { Projection } from "@streamsy/projection";
import { Layer } from "effect";
import { initialiseSearch, search } from "./search-index.ts";

/**
 * Local Files commits select their search binding. Each commit also writes a
 * pending mark in its transaction, so a restart runs the folders that a stop
 * left behind, removals included, without a scan of every folder.
 */
export const IndexLive = Projection.attach(search, { durable: true }).pipe(
  Layer.provide(Layer.effectDiscard(initialiseSearch)),
);
