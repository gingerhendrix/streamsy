import { Layer } from "effect";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Transaction } from "@streamsy/projection";
import { group } from "./shop.ts";

export const host = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layer(group, { client: { filename: ":memory:" }, readLimit: 200 })),
);
